package queue

import (
	"errors"
	"time"
)

var (
	ErrStarted     = errors.New("Already started")
	ErrClosed      = errors.New("Consumer is closed")
	ErrTimeout     = errors.New("Timeout")
	ErrNoSuchQueue = errors.New("No such queue")
	ErrUnsupported = errors.New("Unsupported")
)

type Attributes map[string]string

type Message struct {
	Attributes Attributes `json:"attributes,omitempty"`
	Data       []byte     `json:"data"`
}

type Delivery interface {
	Message() *Message
	Ack()
	Nack()
}

type Consumer interface {
	Receive() (Delivery, error)
	ReceiveWithTimeout(time.Duration) (Delivery, error)
	Close() error
}

type Queue interface {
	Publish(*Message) error
	Consumer(name string) (Consumer, error)
	Close() error
}
