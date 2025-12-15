package pulse

import (
	"context"
)

// Dispatcher is the interface that event handlers must implement.
// The core plugin uses this abstraction to remain decoupled from
// specific messaging systems (Typesense, NATS, Kafka, Redis, etc.).
type Dispatcher interface {
	// Dispatch sends an event to the external system.
	// Implementations should handle their own error recovery and retries.
	// The context can be used for cancellation and timeouts.
	Dispatch(ctx context.Context, event PulseEvent) error
}

// DispatcherFunc is a function adapter for the Dispatcher interface.
// This allows using simple functions as dispatchers for testing or simple use cases.
type DispatcherFunc func(ctx context.Context, event PulseEvent) error

// Dispatch implements the Dispatcher interface.
func (f DispatcherFunc) Dispatch(ctx context.Context, event PulseEvent) error {
	return f(ctx, event)
}

// MultiDispatcher fans out events to multiple dispatchers.
// Useful when you need to send events to multiple systems simultaneously.
type MultiDispatcher struct {
	dispatchers []Dispatcher
	// ContinueOnError determines if dispatch continues when one dispatcher fails.
	ContinueOnError bool
}

// NewMultiDispatcher creates a dispatcher that sends to multiple targets.
func NewMultiDispatcher(dispatchers ...Dispatcher) *MultiDispatcher {
	return &MultiDispatcher{
		dispatchers:     dispatchers,
		ContinueOnError: true, // Default: continue even if one fails
	}
}

// Dispatch sends the event to all registered dispatchers.
func (m *MultiDispatcher) Dispatch(ctx context.Context, event PulseEvent) error {
	var firstErr error
	for _, d := range m.dispatchers {
		if err := d.Dispatch(ctx, event); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if !m.ContinueOnError {
				return err
			}
		}
	}
	return firstErr
}

// NoOpDispatcher is a dispatcher that does nothing.
// Useful for testing or disabling event emission.
type NoOpDispatcher struct{}

// Dispatch implements Dispatcher but performs no action.
func (NoOpDispatcher) Dispatch(_ context.Context, _ PulseEvent) error {
	return nil
}

// LoggingDispatcher wraps a dispatcher and logs events.
// Useful for debugging and development.
type LoggingDispatcher struct {
	Inner  Dispatcher
	Logger func(event PulseEvent)
}

// Dispatch logs the event and forwards to the inner dispatcher.
func (l *LoggingDispatcher) Dispatch(ctx context.Context, event PulseEvent) error {
	if l.Logger != nil {
		l.Logger(event)
	}
	if l.Inner != nil {
		return l.Inner.Dispatch(ctx, event)
	}
	return nil
}
