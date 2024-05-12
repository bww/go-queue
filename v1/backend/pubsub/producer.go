package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/bww/go-queue/v1"
	"github.com/bww/go-queue/v1/config"

	"cloud.google.com/go/pubsub"
	"github.com/bww/go-gcputil/auth"
	"google.golang.org/api/option"
)

const Scheme = "pubsub"

const defaultBacklog = 20

func subscrConfig(topic *pubsub.Topic) pubsub.SubscriptionConfig {
	return pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Minute,
	}
}

type outbound struct {
	Res *pubsub.PublishResult
	Pub time.Time
}

func newOutbound(res *pubsub.PublishResult, pub time.Time) outbound {
	return outbound{
		Res: res,
		Pub: pub,
	}
}

type backend struct {
	config.Config
	cxt    context.Context
	closer context.CancelFunc
	client *pubsub.Client
	topic  *pubsub.Topic
	log    *slog.Logger
	outbox chan outbound
	create bool // create topics and subscriptions if they don't exist
}

func New(dsn string, opts ...config.Option) (*backend, error) {
	return NewWithConfig(dsn, config.Config{}.WithOptions(opts...))
}

func NewWithConfig(dsn string, conf config.Config) (*backend, error) {
	return NewWithContext(context.Background(), dsn, conf)
}

func NewWithContext(cxt context.Context, dsn string, conf config.Config) (*backend, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	var projectId string
	var opts []option.ClientOption
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		creds, pcxt, err := auth.Credentials(dsn, pubsub.ScopePubSub)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithCredentials(creds))
		projectId = pcxt.ProjectId
	} else {
		projectId = os.Getenv("PUBSUB_PROJECT_ID")
	}

	client, err := pubsub.NewClient(context.Background(), projectId, opts...)
	if err != nil {
		return nil, err
	}

	query := u.Query()
	create := strings.ToLower(query.Get("create")) == "true"

	tname := u.Host
	topic := client.Topic(tname)
	if n := conf.Backlog; n > 0 {
		topic.PublishSettings.CountThreshold = n
	}

	exists, err := topic.Exists(context.Background())
	if err != nil {
		return nil, err
	}
	if !exists {
		if !create {
			return nil, queue.ErrNoSuchQueue
		}
		topic, err = client.CreateTopic(context.Background(), tname)
		if err != nil {
			return nil, err
		}
	}

	log := slog.Default().With("name", "pubsub", "topic", tname)
	var outbox chan outbound
	if conf.Synchronous {
		log = log.With("mode", "sync")
	} else {
		log = log.With("mode", "async")
		outbox = make(chan outbound, defaultBacklog)
	}

	if cxt == nil {
		cxt = context.Background()
	}
	cxt, closer := context.WithCancel(cxt)

	b := &backend{
		Config: conf,
		cxt:    cxt,
		closer: closer,
		client: client,
		topic:  topic,
		log:    log,
		outbox: outbox,
		create: create,
	}

	if outbox != nil {
		go notify(cxt, outbox, log, conf)
	}
	return b, nil
}

func (b *backend) Consumer(dsn string) (queue.Consumer, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if u.Scheme != Scheme {
		return nil, queue.ErrUnsupported
	}

	name := u.Host
	sub := b.client.Subscription(name)
	exists, err := sub.Exists(context.Background())
	if err != nil {
		return nil, err
	}
	if !exists {
		if !b.create {
			return nil, queue.ErrNoSuchQueue
		}
		sub, err = b.client.CreateSubscription(context.Background(), name, subscrConfig(b.topic))
		if err != nil {
			return nil, fmt.Errorf("Could not create subscription: %w", err)
		}
	}

	c := newConsumer(sub)
	c.start()
	return c, nil
}

func (b *backend) Publish(message *queue.Message) error {
	now := time.Now()
	psm := &pubsub.Message{
		Data:        message.Data,
		Attributes:  message.Attributes,
		PublishTime: now,
	}
	res := b.topic.Publish(context.Background(), psm)
	// in syncrhonous mode, we wait on this routine for a response from the
	// service and return it; otherwise, we enqueue the message and wait for a
	// response on another routine to avoid blocking.
	if b.Synchronous {
		id, err := res.Get(context.Background())
		if err != nil {
			return fmt.Errorf("Could not publish message: %w", err)
		}
		b.log.Debug("Published message", "id", id)
	} else {
		b.monitor(newOutbound(res, now))
	}
	return nil
}

func (b *backend) Close() error {
	b.closer()
	return nil
}

func (b *backend) monitor(res outbound) {
	if b.outbox != nil { // outbox is immutable after creation
		b.outbox <- res
	}
}

func notify(cxt context.Context, resv <-chan outbound, log *slog.Logger, conf config.Config) {
	log.Debug("Starting outbox monitor...")
	defer log.Debug("Outbox monitor is shutting down...")
	for {
		var out outbound
		var ok bool
		select {
		case <-cxt.Done():
			break // context canceled,we're done
		case out, ok = <-resv:
			if !ok {
				break // channel closed; we're done
			}
		}
		id, err := out.Res.Get(context.Background())
		if err != nil {
			log.With("cause", err).Error("Could not publish message")
		} else {
			log.With("id", id, "delay", time.Since(out.Pub)).Debug("Published message")
		}
	}
}
