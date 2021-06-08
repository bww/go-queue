package pubsub

import (
	"context"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bww/go-queue"
	"github.com/bww/go-queue/config"

	"cloud.google.com/go/pubsub"
	"github.com/bww/go-gcputil/auth"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

const Scheme = "pubsub"

const defaultBacklog = 20

type delivery struct {
	message *queue.Message
	origin  *pubsub.Message
}

func (d *delivery) Message() *queue.Message {
	return d.message
}

func (d *delivery) Ack() {
	d.origin.Ack()
}

func (d *delivery) Nack() {
	d.origin.Nack()
}

type consumer struct {
	sync.Mutex
	subscr   *pubsub.Subscription
	cancel   context.CancelFunc
	delivery chan *delivery
	err      error
}

func newConsumer(sub *pubsub.Subscription) *consumer {
	return &consumer{
		subscr:   sub,
		delivery: make(chan *delivery, defaultBacklog),
	}
}

func (c *consumer) start() error {
	c.Lock()
	defer c.Unlock()
	if c.cancel != nil {
		return queue.ErrStarted
	}
	var cxt context.Context
	cxt, c.cancel = context.WithCancel(context.Background())
	go func() {
		err := c.subscr.Receive(cxt, c.receive)
		c.Lock()
		if err == context.Canceled {
			c.err = queue.ErrClosed
		} else {
			c.err = err
		}
		c.cancel = nil
		close(c.delivery)
		c.Unlock()
	}()
	return nil
}

func (c *consumer) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.cancel == nil {
		return queue.ErrClosed
	}
	c.cancel()
	c.cancel = nil
	return nil
}

func (c *consumer) receive(cxt context.Context, origin *pubsub.Message) {
	var attrs queue.Attributes
	if origin.Attributes != nil {
		attrs = origin.Attributes
	} else {
		attrs = make(queue.Attributes)
	}
	message := &queue.Message{
		Attributes: attrs,
		Data:       origin.Data,
	}
	elem := &delivery{
		message: message,
		origin:  origin,
	}
	select {
	case <-cxt.Done():
		return // we're cancelled
	default:
		c.delivery <- elem
	}
}

func (c *consumer) checkerr() error {
	c.Lock()
	err := c.err
	c.Unlock()
	return err
}

func (c *consumer) Receive() (queue.Delivery, error) {
	err := c.checkerr()
	if err != nil {
		return nil, err
	}
	task, ok := <-c.delivery
	if ok {
		return task, nil
	} else {
		return nil, queue.ErrClosed
	}
}

func (c *consumer) ReceiveWithTimeout(timeout time.Duration) (queue.Delivery, error) {
	err := c.checkerr()
	if err != nil {
		return nil, err
	}
	select {
	case <-time.After(timeout):
		return nil, queue.ErrTimeout
	case task, ok := <-c.delivery:
		if ok {
			return task, nil
		} else {
			return nil, queue.ErrClosed
		}
	}
}

func subscrConfig(topic *pubsub.Topic) pubsub.SubscriptionConfig {
	return pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Minute,
	}
}

type backend struct {
	config.Config
	client *pubsub.Client
	topic  *pubsub.Topic
	log    *logrus.Entry
	create bool // create topics and subscriptions if they don't exist
}

func New(dsn string, opts ...config.Option) (*backend, error) {
	return NewWithConfig(dsn, config.Config{}.WithOptions(opts...))
}

func NewWithConfig(dsn string, conf config.Config) (*backend, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	var projectId string
	var opts []option.ClientOption
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		creds, cxt, err := auth.Credentials(dsn, pubsub.ScopePubSub)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithCredentials(creds))
		projectId = cxt.ProjectId
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

	return &backend{
		Config: conf,
		client: client,
		topic:  topic,
		log:    logrus.WithFields(logrus.Fields{"queue": "pubsub", "topic": tname}),
		create: create,
	}, nil
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
			return nil, err
		}
	}

	c := newConsumer(sub)
	c.start()
	return c, nil
}

func (b *backend) Publish(message *queue.Message) error {
	res := b.topic.Publish(context.Background(), &pubsub.Message{
		Data:        message.Data,
		Attributes:  message.Attributes,
		PublishTime: time.Now(),
	})
	if b.Synchronous {
		id, err := res.Get(context.Background())
		if err != nil {
			return err
		}
		if b.Debug {
			b.log.Println("Published message:", id)
		}
	}
	return nil
}
