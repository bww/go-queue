package backend

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/bww/go-queue/v1"
	"github.com/bww/go-queue/v1/backend/blackhole"
	"github.com/bww/go-queue/v1/backend/pubsub"
	"github.com/bww/go-queue/v1/config"
)

func New(dsn string, opts ...config.Option) (queue.Queue, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case pubsub.Scheme:
		return pubsub.New(dsn, opts...)
	case blackhole.Scheme:
		return blackhole.New(dsn)
	default:
		return nil, fmt.Errorf("Unsupported queue backend: %s", dsn)
	}
}
