package pulse

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestDispatcherFunc tests the function adapter.
func TestDispatcherFunc(t *testing.T) {
	called := false
	var receivedEvent PulseEvent

	fn := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		called = true
		receivedEvent = event
		return nil
	})

	event := PulseEvent{Type: PulseCreate, Table: "test"}
	err := fn.Dispatch(context.Background(), event)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected function to be called")
	}
	if receivedEvent.Type != PulseCreate {
		t.Errorf("expected event type create, got %s", receivedEvent.Type)
	}
}

// TestDispatcherFuncError tests error handling in function adapter.
func TestDispatcherFuncError(t *testing.T) {
	expectedErr := errors.New("test error")
	fn := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		return expectedErr
	})

	err := fn.Dispatch(context.Background(), PulseEvent{})
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

// TestNoOpDispatcher tests the no-op dispatcher.
func TestNoOpDispatcher(t *testing.T) {
	d := NoOpDispatcher{}
	err := d.Dispatch(context.Background(), PulseEvent{Type: PulseCreate})

	if err != nil {
		t.Errorf("NoOpDispatcher should not return error, got %v", err)
	}
}

// TestMultiDispatcher tests fan-out to multiple dispatchers.
func TestMultiDispatcher(t *testing.T) {
	var count1, count2 int32

	d1 := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		atomic.AddInt32(&count1, 1)
		return nil
	})

	d2 := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		atomic.AddInt32(&count2, 1)
		return nil
	})

	multi := NewMultiDispatcher(d1, d2)
	event := PulseEvent{Type: PulseCreate}

	if err := multi.Dispatch(context.Background(), event); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if atomic.LoadInt32(&count1) != 1 {
		t.Errorf("expected d1 to be called once, got %d", count1)
	}
	if atomic.LoadInt32(&count2) != 1 {
		t.Errorf("expected d2 to be called once, got %d", count2)
	}
}

// TestMultiDispatcherContinueOnError tests that multi-dispatcher continues on error.
func TestMultiDispatcherContinueOnError(t *testing.T) {
	var count int32
	expectedErr := errors.New("first dispatcher error")

	d1 := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		atomic.AddInt32(&count, 1)
		return expectedErr
	})

	d2 := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	multi := NewMultiDispatcher(d1, d2)
	multi.ContinueOnError = true

	err := multi.Dispatch(context.Background(), PulseEvent{})

	// Should return first error but continue
	if err != expectedErr {
		t.Errorf("expected first error, got %v", err)
	}

	// Both dispatchers should have been called
	if atomic.LoadInt32(&count) != 2 {
		t.Errorf("expected both dispatchers to be called, got %d calls", count)
	}
}

// TestMultiDispatcherStopOnError tests that multi-dispatcher can stop on error.
func TestMultiDispatcherStopOnError(t *testing.T) {
	var count int32
	expectedErr := errors.New("stop error")

	d1 := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		atomic.AddInt32(&count, 1)
		return expectedErr
	})

	d2 := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	multi := NewMultiDispatcher(d1, d2)
	multi.ContinueOnError = false

	err := multi.Dispatch(context.Background(), PulseEvent{})

	if err != expectedErr {
		t.Errorf("expected error, got %v", err)
	}

	// Only first dispatcher should have been called
	if atomic.LoadInt32(&count) != 1 {
		t.Errorf("expected only first dispatcher to be called, got %d calls", count)
	}
}

// TestLoggingDispatcher tests the logging wrapper.
func TestLoggingDispatcher(t *testing.T) {
	var loggedEvent PulseEvent
	var innerCalled bool

	inner := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		innerCalled = true
		return nil
	})

	logging := &LoggingDispatcher{
		Inner: inner,
		Logger: func(event PulseEvent) {
			loggedEvent = event
		},
	}

	event := PulseEvent{Type: PulseUpdate, Table: "users"}
	err := logging.Dispatch(context.Background(), event)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !innerCalled {
		t.Error("expected inner dispatcher to be called")
	}
	if loggedEvent.Type != PulseUpdate {
		t.Errorf("expected logged event type update, got %s", loggedEvent.Type)
	}
	if loggedEvent.Table != "users" {
		t.Errorf("expected logged event table 'users', got %s", loggedEvent.Table)
	}
}

// TestLoggingDispatcherNilInner tests logging dispatcher with nil inner.
func TestLoggingDispatcherNilInner(t *testing.T) {
	var logCalled bool

	logging := &LoggingDispatcher{
		Inner: nil,
		Logger: func(event PulseEvent) {
			logCalled = true
		},
	}

	err := logging.Dispatch(context.Background(), PulseEvent{})

	if err != nil {
		t.Errorf("unexpected error with nil inner: %v", err)
	}
	if !logCalled {
		t.Error("expected logger to be called even with nil inner")
	}
}

// TestLoggingDispatcherNilLogger tests logging dispatcher with nil logger.
func TestLoggingDispatcherNilLogger(t *testing.T) {
	var innerCalled bool

	inner := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		innerCalled = true
		return nil
	})

	logging := &LoggingDispatcher{
		Inner:  inner,
		Logger: nil,
	}

	err := logging.Dispatch(context.Background(), PulseEvent{})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !innerCalled {
		t.Error("expected inner to be called even with nil logger")
	}
}

// TestMultiDispatcherEmpty tests multi-dispatcher with no dispatchers.
func TestMultiDispatcherEmpty(t *testing.T) {
	multi := NewMultiDispatcher()

	err := multi.Dispatch(context.Background(), PulseEvent{})
	if err != nil {
		t.Errorf("expected no error for empty multi-dispatcher, got %v", err)
	}
}
