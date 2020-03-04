package backend

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/bww/go-queue"
	"github.com/bww/go-queue/backend/blackhole"
	"github.com/bww/go-queue/backend/pubsub"
)

func New(dsn string) (queue.Queue, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case pubsub.Scheme:
		return pubsub.New(dsn)
	case blackhole.Scheme:
		return blackhole.New(dsn)
	default:
		return nil, fmt.Errorf("Unsupported queue backend: %s", dsn)
	}
}
