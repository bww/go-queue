package blackhole

import (
	"time"

	"github.com/bww/v1/go-queue"
)

const Scheme = "blackhole"

type delivery struct {
	message *queue.Message
}

func (d *delivery) Message() *queue.Message {
	return d.message
}

func (d *delivery) Ack() {
	// nothing
}

func (d *delivery) Nack() {
	// nothing
}

type consumer struct {
	delivery chan *delivery
}

func (c *consumer) Close() error {
	return nil
}

func (c *consumer) Receive() (queue.Delivery, error) {
	d := <-c.delivery
	return d, nil
}

func (c *consumer) ReceiveWithTimeout(timeout time.Duration) (queue.Delivery, error) {
	select {
	case <-time.After(timeout):
		return nil, queue.ErrTimeout
	case <-c.delivery:
		return nil, nil
	}
}

type backend struct{}

func New(dsn string) (*backend, error) {
	return &backend{}, nil
}

func (b *backend) Consumer(dsn string) (queue.Consumer, error) {
	return &consumer{make(chan *delivery)}, nil
}

func (b *backend) Publish(task *queue.Message) error {
	return nil
}

func (b *backend) Close() error {
	return nil
}
