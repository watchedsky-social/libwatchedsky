package libwatchedsky

import "errors"

var (
	// ErrNilContext is returned whenever a func that expects a [context.Context]
	// receives a nil value
	ErrNilContext = errors.New("context cannot be nil")
)
