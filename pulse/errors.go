package pulse

import "errors"

var (
	// ErrNoDispatcher is returned when no dispatcher is configured.
	ErrNoDispatcher = errors.New("gormpulse: dispatcher is required")

	// ErrOutboxNotInitialized is returned when outbox operations are attempted
	// before the outbox table has been created.
	ErrOutboxNotInitialized = errors.New("gormpulse: outbox table not initialized, call AutoMigrate first")

	// ErrWorkerAlreadyRunning is returned when starting a worker that's already running.
	ErrWorkerAlreadyRunning = errors.New("gormpulse: outbox worker is already running")

	// ErrWorkerNotRunning is returned when stopping a worker that's not running.
	ErrWorkerNotRunning = errors.New("gormpulse: outbox worker is not running")
)
