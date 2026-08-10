package callwrapper

import "errors"

type CallwrapperError struct {
	MainError  error
	CacheError error
}

func (c *CallwrapperError) ErrorExist() bool {
	return c.MainError != nil || c.CacheError != nil
}

var (
	ErrCircuitOpen = errors.New("callwrapper: circuit breaker is open")
	ErrNilFunction = errors.New("callwrapper: function is nil")
)
