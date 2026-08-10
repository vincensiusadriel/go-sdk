package callwrapper

import (
	"context"
	"time"

	"github.com/cep21/circuit/v4"
	"github.com/cep21/circuit/v4/closers/hystrix"
	"github.com/vincensiusadriel/go-sdk/cache"
	"golang.org/x/sync/singleflight"
)

type Callwrapper[V any] struct {
	timeout time.Duration

	cache    cache.Cache[V]
	cacheTTL time.Duration

	singleflight *singleflight.Group

	circuitBreaker *circuit.Circuit
}

func (c *Callwrapper[V]) WithTimeout(timeout time.Duration) *Callwrapper[V] {
	c.timeout = timeout
	return c
}

func (c *Callwrapper[V]) WithCache(cache cache.Cache[V], cacheTTL time.Duration) *Callwrapper[V] {
	c.cache = cache
	c.cacheTTL = cacheTTL
	return c
}

func (c *Callwrapper[V]) WithSingleflight(useSingleflight bool) *Callwrapper[V] {
	if useSingleflight {
		c.singleflight = &singleflight.Group{}
	}
	return c
}

var defaultCBConfig = CircuitBreakerConfig{
	Opener: CircuitBreakerOpenerConfig{
		ErrorThresholdPercentage: 80,
		RequestVolumeThreshold:   20,
		RollingDuration:          10 * time.Second,
	},
	Closer: CircuitBreakerCloserConfig{
		SleepWindow:                  3 * time.Second,
		HalfOpenAttempts:             1,
		RequiredConcurrentSuccessful: 1,
	},

	MaxConcurrentRequests: 10,
}

type CircuitBreakerConfig struct {
	Opener                CircuitBreakerOpenerConfig
	Closer                CircuitBreakerCloserConfig
	MaxConcurrentRequests int64
	Timeout               time.Duration
}

type CircuitBreakerOpenerConfig struct {
	ErrorThresholdPercentage int64
	RequestVolumeThreshold   int64
	RollingDuration          time.Duration
}

type CircuitBreakerCloserConfig struct {
	SleepWindow                  time.Duration
	HalfOpenAttempts             int64
	RequiredConcurrentSuccessful int64
}

// NewCallWrapper creates an empty Callwrapper[V]. Configure it by chaining
// WithTimeout, WithCache, WithSingleflight, and WithCircuitBreaker. Create
// one wrapper per service function (typically in init or a package var) and
// reuse it for every invocation of that function so circuit breaker stats
// accumulate across callers.
func NewCallWrapper[V any]() *Callwrapper[V] {
	return &Callwrapper[V]{}
}

func (c *Callwrapper[V]) WithCircuitBreaker(useCircuitBreaker bool, config ...CircuitBreakerConfig) *Callwrapper[V] {
	if !useCircuitBreaker {
		c.circuitBreaker = nil
		return c
	}

	cbConfig := defaultCBConfig
	if len(config) > 0 {
		cbConfig = config[0]
	}

	producer := hystrix.Factory{
		ConfigureOpener: hystrix.ConfigureOpener{
			ErrorThresholdPercentage: cbConfig.Opener.ErrorThresholdPercentage,
			RequestVolumeThreshold:   cbConfig.Opener.RequestVolumeThreshold,
			RollingDuration:          cbConfig.Opener.RollingDuration,
		},
		ConfigureCloser: hystrix.ConfigureCloser{
			SleepWindow:                  cbConfig.Closer.SleepWindow,
			HalfOpenAttempts:             cbConfig.Closer.HalfOpenAttempts,
			RequiredConcurrentSuccessful: cbConfig.Closer.RequiredConcurrentSuccessful,
		},
	}

	manager := circuit.Manager{
		DefaultCircuitProperties: []circuit.CommandPropertiesConstructor{producer.Configure},
	}

	c.circuitBreaker = manager.MustCreateCircuit("callwrapper", circuit.Config{
		Execution: circuit.ExecutionConfig{
			MaxConcurrentRequests: cbConfig.MaxConcurrentRequests,
			Timeout:               cbConfig.Timeout,
		},
	})

	return c
}

func (c *Callwrapper[V]) Call(ctx context.Context, key string, exec func(context.Context) (V, error)) (V, CallwrapperError) {
	if c.singleflight != nil {
		return c.call(ctx, key, func(ctx context.Context) (V, error) {
			singleflightresp, err, _ := c.singleflight.Do(key, func() (any, error) {
				return exec(ctx)
			})
			resp, _ := singleflightresp.(V)
			return resp, err
		})
	} else {
		return c.call(ctx, key, exec)
	}
}

func (c *Callwrapper[V]) call(ctx context.Context, key string, exec func(context.Context) (V, error)) (res V, wrapperError CallwrapperError) {
	if exec == nil {
		wrapperError.MainError = ErrNilFunction
		return res, wrapperError
	}

	if err := ctx.Err(); err != nil {
		wrapperError.MainError = err
		return res, wrapperError
	}

	if c.timeout > 0 {
		ctxtimeout, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		ctx = ctxtimeout
	}

	if c.cache != nil {
		value, found, errCache := c.cache.Get(ctx, key)
		if errCache != nil {
			wrapperError.CacheError = errCache
		}

		if found {
			return value, wrapperError
		}
	}

	if c.circuitBreaker != nil {
		runErr := c.circuitBreaker.Execute(ctx, func(innerCtx context.Context) error {
			var err error
			res, err = exec(innerCtx)
			return err
		}, nil)
		if runErr != nil {
			if circuitErr, ok := runErr.(circuit.Error); ok && circuitErr.CircuitOpen() {
				wrapperError.MainError = ErrCircuitOpen
			} else {
				wrapperError.MainError = runErr
			}
			return res, wrapperError
		}
	} else {
		var err error
		res, err = exec(ctx)
		if err != nil {
			wrapperError.MainError = err
			return res, wrapperError
		}
	}

	select {
	case <-ctx.Done():
		wrapperError.MainError = ctx.Err()
		return res, wrapperError
	default:
	}

	if c.cache != nil && !wrapperError.ErrorExist() {
		errCache := c.cache.Set(ctx, key, res, c.cacheTTL)
		if errCache != nil {
			wrapperError.CacheError = errCache
		}
	}

	return res, wrapperError
}
