package cache

//go:generate mockgen -source=interface.go -destination=mockcache/cache.go -package=mockcache

import (
	"context"
	"time"
)

type Cache[V any] interface {
	Get(ctx context.Context, key string) (V, bool, error)
	Set(ctx context.Context, key string, value V, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}
