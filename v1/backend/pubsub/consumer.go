package pubsub

import (
	"context"
	"sync"
	"time"

	"github.com/bww/go-queue/v1"

	"cloud.google.com/go/pubsub"
)

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
