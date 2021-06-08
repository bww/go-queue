package pubsub

import (
	"fmt"
	"testing"
	"time"

	"github.com/bww/go-queue"
	"github.com/bww/go-queue/config"

	"github.com/bww/go-util/v1/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	testDSN = "pubsub://queue-pubsub-backend?create=true"
	testSub = "pubsub://queue-pubsub-backend-default"
)

func TestPubSub(t *testing.T) {
	count := 100
	tests := make([]*queue.Message, 0, count)
	for i := 0; i < count; i++ {
		tests = append(tests, &queue.Message{
			Attributes: queue.Attributes{
				"id":  uuid.New().String(),
				"url": "http://www.google.com/",
			},
			Data: []byte(fmt.Sprintf("This is task #%d", i+1)),
		})
	}

	q, err := New(testDSN)
	if !assert.Nil(t, err, fmt.Sprint(err)) {
		return
	}

	sent := make(map[string]*queue.Message)
	for _, e := range tests {
		err := q.Publish(e)
		if assert.Nil(t, err, fmt.Sprint(err)) {
			sent[e.Attributes["id"]] = e
		}
	}

	consumer, err := q.Consumer(testSub)
	if !assert.Nil(t, err, fmt.Sprint(err)) {
		return
	}

	recv := make(map[string]*queue.Message)
	for {
		delivery, err := consumer.ReceiveWithTimeout(time.Second / 2)
		if err == queue.ErrTimeout {
			break
		}
		if assert.Nil(t, err, fmt.Sprint(err)) {
			delivery.Ack()
			msg := delivery.Message()
			recv[msg.Attributes["id"]] = msg
		}
	}

	if assert.Equal(t, len(sent), len(recv)) {
		for k, e := range sent {
			msg, ok := recv[k]
			if assert.True(t, ok, fmt.Sprintf("Message not receieved: %v", e.Attributes["id"])) {
				assert.Equal(t, e, msg)
			}
		}
	}

	err = consumer.Close()
	assert.Nil(t, err, fmt.Sprint(err))

	_, err = consumer.Receive()
	assert.Equal(t, queue.ErrClosed, err)
}

func TestPubSubOptions(t *testing.T) {
	count := 100
	tests := make([]*queue.Message, 0, count)
	for i := 0; i < count; i++ {
		tests = append(tests, &queue.Message{
			Attributes: queue.Attributes{
				"id":  uuid.New().String(),
				"url": "http://www.google.com/",
			},
			Data: []byte(fmt.Sprintf("This is task #%d", i+1)),
		})
	}

	q, err := New(testDSN, config.Synchronous(true), config.Debug(true))
	if !assert.Nil(t, err, fmt.Sprint(err)) {
		return
	}

	sent := make(map[string]*queue.Message)
	for _, e := range tests {
		err := q.Publish(e)
		if assert.Nil(t, err, fmt.Sprint(err)) {
			sent[e.Attributes["id"]] = e
		}
	}

	consumer, err := q.Consumer(testSub)
	if !assert.Nil(t, err, fmt.Sprint(err)) {
		return
	}

	recv := make(map[string]*queue.Message)
	for {
		delivery, err := consumer.ReceiveWithTimeout(time.Second / 2)
		if err == queue.ErrTimeout {
			break
		}
		if assert.Nil(t, err, fmt.Sprint(err)) {
			delivery.Ack()
			msg := delivery.Message()
			recv[msg.Attributes["id"]] = msg
		}
	}

	if assert.Equal(t, len(sent), len(recv)) {
		for k, e := range sent {
			msg, ok := recv[k]
			if assert.True(t, ok, fmt.Sprintf("Message not receieved: %v", e.Attributes["id"])) {
				assert.Equal(t, e, msg)
			}
		}
	}

	err = consumer.Close()
	assert.Nil(t, err, fmt.Sprint(err))

	_, err = consumer.Receive()
	assert.Equal(t, queue.ErrClosed, err)
}
