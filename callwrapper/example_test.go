package callwrapper_test

import (
	"context"
	"fmt"
	"time"

	"github.com/vincensiusadriel/go-sdk/callwrapper"
)

// listOrderResponse is the response type of the order.ListOrder service.
type listOrderResponse struct {
	Orders int
}

// listOrder is created once (package level), and reused for every
// order.ListOrder invocation. Because it lives for the process lifetime,
// circuit breaker stats accumulate across all callers of this service.
var listOrder = callwrapper.NewCallWrapper[listOrderResponse]().
	WithTimeout(5 * time.Second).
	WithSingleflight(true).
	WithCircuitBreaker(true, callwrapper.CircuitBreakerConfig{
		Opener: callwrapper.CircuitBreakerOpenerConfig{
			RequestVolumeThreshold:   5,
			ErrorThresholdPercentage: 50,
		},
	})

// orderService is a client for the order service.
type orderService struct{}

// ListOrder is called on every incoming order.ListOrder request.
func (orderService) ListOrder(ctx context.Context) (listOrderResponse, error) {
	res, wErr := listOrder.Call(ctx, "order.ListOrder", func(ctx context.Context) (listOrderResponse, error) {
		// The actual RPC call goes here.
		return listOrderResponse{Orders: 42}, nil
	})
	return res, wErr.MainError
}

func ExampleNewCallWrapper() {
	var svc orderService
	res, err := svc.ListOrder(context.Background())
	if err != nil {
		fmt.Println("failed:", err)
		return
	}
	fmt.Printf("orders=%d\n", res.Orders)
	// Output: orders=42
}

