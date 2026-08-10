package callwrapper

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vincensiusadriel/go-sdk/cache/mockcache"
	"go.uber.org/mock/gomock"
)

func TestCallSuccess(t *testing.T) {
	var c Callwrapper[string]

	res, wErr := c.Call(context.Background(), "k", func(ctx context.Context) (string, error) {
		return "hello", nil
	})

	if wErr.MainError != nil {
		t.Fatalf("unexpected error: %v", wErr.MainError)
	}
	if res != "hello" {
		t.Fatalf("expected hello, got %q", res)
	}
}

func TestCallNilFunction(t *testing.T) {
	var c Callwrapper[string]

	_, wErr := c.Call(context.Background(), "k", nil)

	if !errors.Is(wErr.MainError, ErrNilFunction) {
		t.Fatalf("expected ErrNilFunction, got %v", wErr.MainError)
	}
}

func TestCallCanceledContext(t *testing.T) {
	var c Callwrapper[string]

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	execCalled := false
	_, wErr := c.Call(ctx, "k", func(ctx context.Context) (string, error) {
		execCalled = true
		return "hello", nil
	})

	if wErr.MainError != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", wErr.MainError)
	}
	if execCalled {
		t.Fatal("exec should not be called on canceled context")
	}
}

func TestCallTimeout(t *testing.T) {
	c := new(Callwrapper[string]).WithTimeout(50 * time.Millisecond)

	_, wErr := c.Call(context.Background(), "k", func(ctx context.Context) (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "late", nil
	})

	if !errors.Is(wErr.MainError, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", wErr.MainError)
	}
}

func TestCallCacheHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cacheMock := mockcache.NewMockCache[string](ctrl)
	cacheMock.EXPECT().Get(gomock.Any(), "k").Return("cached", true, nil)

	c := NewCallWrapper[string]().WithCache(cacheMock, time.Minute)

	execCalls := 0
	res, wErr := c.Call(context.Background(), "k", func(ctx context.Context) (string, error) {
		execCalls++
		return "fresh", nil
	})

	if wErr.MainError != nil {
		t.Fatalf("unexpected error: %v", wErr.MainError)
	}
	if res != "cached" {
		t.Fatalf("expected cached value, got %q", res)
	}
	if execCalls != 0 {
		t.Fatalf("expected exec not to run on cache hit, ran %d times", execCalls)
	}
}

func TestCallCacheSetOnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cacheMock := mockcache.NewMockCache[string](ctrl)
	cacheMock.EXPECT().Get(gomock.Any(), "k").Return("", false, nil)
	cacheMock.EXPECT().Set(gomock.Any(), "k", "fresh", time.Minute).Return(nil)

	c := NewCallWrapper[string]().WithCache(cacheMock, time.Minute)

	res, wErr := c.Call(context.Background(), "k", func(ctx context.Context) (string, error) {
		return "fresh", nil
	})

	if wErr.MainError != nil {
		t.Fatalf("unexpected error: %v", wErr.MainError)
	}
	if wErr.CacheError != nil {
		t.Fatalf("unexpected cache error: %v", wErr.CacheError)
	}
	if res != "fresh" {
		t.Fatalf("expected fresh value, got %q", res)
	}
}

func TestCallCacheGetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cacheErr := errors.New("cache down")
	cacheMock := mockcache.NewMockCache[string](ctrl)
	cacheMock.EXPECT().Get(gomock.Any(), "k").Return("", false, cacheErr)

	c := NewCallWrapper[string]().WithCache(cacheMock, time.Minute)

	res, wErr := c.Call(context.Background(), "k", func(ctx context.Context) (string, error) {
		return "fresh", nil
	})

	if wErr.MainError != nil {
		t.Fatalf("unexpected error: %v", wErr.MainError)
	}
	if !errors.Is(wErr.CacheError, cacheErr) {
		t.Fatalf("expected cache error %v, got %v", cacheErr, wErr.CacheError)
	}
	if res != "fresh" {
		t.Fatalf("expected fresh value despite cache error, got %q", res)
	}
}

func TestCallSingleflightDeduplicates(t *testing.T) {
	c := NewCallWrapper[string]().WithSingleflight(true)

	var execCalls atomic.Int32
	release := make(chan struct{})
	exec := func(ctx context.Context) (string, error) {
		execCalls.Add(1)
		<-release
		return "shared", nil
	}

	const n = 10
	var wg sync.WaitGroup
	results := make([]CallwrapperError, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var res string
			res, results[i] = c.Call(context.Background(), "same-key", exec)
			_ = res
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	if got := execCalls.Load(); got != 1 {
		t.Fatalf("expected exec to run once under singleflight, ran %d times", got)
	}

	close(release)
	wg.Wait()

	for i, rErr := range results {
		if rErr.MainError != nil {
			t.Fatalf("caller %d got unexpected error: %v", i, rErr.MainError)
		}
	}
}

func TestCircuitBreakerOpensAfterFailures(t *testing.T) {
	c := NewCallWrapper[string]().WithCircuitBreaker(true, CircuitBreakerConfig{
		Opener: CircuitBreakerOpenerConfig{
			ErrorThresholdPercentage: 50,
			RequestVolumeThreshold:   1,
			RollingDuration:          10 * time.Second,
		},
		Closer: CircuitBreakerCloserConfig{
			SleepWindow:                  500 * time.Millisecond,
			HalfOpenAttempts:             1,
			RequiredConcurrentSuccessful: 1,
		},
	})

	upstreamErr := errors.New("upstream down")
	var execCalls atomic.Int32
	failingExec := func(ctx context.Context) (string, error) {
		execCalls.Add(1)
		return "", upstreamErr
	}

	_, wErr := c.Call(context.Background(), "k", failingExec)
	if !errors.Is(wErr.MainError, upstreamErr) {
		t.Fatalf("expected upstream error on first call, got %v", wErr.MainError)
	}

	_, wErr = c.Call(context.Background(), "k", failingExec)
	if !errors.Is(wErr.MainError, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", wErr.MainError)
	}

	before := execCalls.Load()
	_, wErr = c.Call(context.Background(), "k", failingExec)
	if !errors.Is(wErr.MainError, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen while circuit open, got %v", wErr.MainError)
	}
	if got := execCalls.Load(); got != before {
		t.Fatalf("exec should not run while circuit is open, ran %d extra times", got-before)
	}
}

func TestCircuitBreakerRecoversAfterSleepWindow(t *testing.T) {
	c := NewCallWrapper[string]().WithCircuitBreaker(true, CircuitBreakerConfig{
		Opener: CircuitBreakerOpenerConfig{
			ErrorThresholdPercentage: 50,
			RequestVolumeThreshold:   1,
			RollingDuration:          10 * time.Second,
		},
		Closer: CircuitBreakerCloserConfig{
			SleepWindow:                  100 * time.Millisecond,
			HalfOpenAttempts:             1,
			RequiredConcurrentSuccessful: 1,
		},
	})

	upstreamErr := errors.New("upstream down")
	_, wErr := c.Call(context.Background(), "k", func(ctx context.Context) (string, error) {
		return "", upstreamErr
	})
	if wErr.MainError == nil {
		t.Fatal("expected error to open the circuit")
	}

	_, wErr = c.Call(context.Background(), "k", func(ctx context.Context) (string, error) {
		return "", upstreamErr
	})
	if !errors.Is(wErr.MainError, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", wErr.MainError)
	}

	var successCalls atomic.Int32
	deadline := time.Now().Add(3 * time.Second)
	for {
		res, rErr := c.Call(context.Background(), "k", func(ctx context.Context) (string, error) {
			successCalls.Add(1)
			return "ok", nil
		})

		if rErr.MainError != nil {
			if !errors.Is(rErr.MainError, ErrCircuitOpen) {
				t.Fatalf("unexpected error: %v", rErr.MainError)
			}
			if time.Now().After(deadline) {
				t.Fatal("circuit never recovered after sleep window")
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}

		if res != "ok" {
			t.Fatalf("expected ok, got %q", res)
		}
		break
	}

	if successCalls.Load() == 0 {
		t.Fatal("expected the circuit to eventually allow execution again")
	}
}