package pulse

import (
	"context"
	"log"
	"time"
)

// SyncMode determines how events are dispatched.
type SyncMode string

const (
	// SyncModeAsync dispatches events in a goroutine (default, non-blocking).
	SyncModeAsync SyncMode = "async"
	// SyncModeSync dispatches events synchronously (blocks until complete).
	SyncModeSync SyncMode = "sync"
)

// Config holds the configuration for the Pulse plugin.
type Config struct {
	// Dispatcher is the handler for emitted events (required).
	Dispatcher Dispatcher

	// SyncMode determines if events are dispatched sync or async.
	// Default: SyncModeAsync
	SyncMode SyncMode

	// UseOutbox enables the outbox pattern for transactional safety.
	// When enabled, events are written to an outbox table within the
	// same transaction, ensuring events are only processed after commit.
	// Default: false (uses in-memory post-commit hooks)
	UseOutbox bool

	// OutboxTableName is the name of the outbox table.
	// Default: "pulse_outbox"
	OutboxTableName string

	// OutboxWorkerInterval is how often the outbox worker checks for pending events.
	// Default: 1 second
	OutboxWorkerInterval time.Duration

	// OutboxBatchSize is the maximum number of events to process per batch.
	// Default: 100
	OutboxBatchSize int

	// OutboxRetryLimit is the maximum number of retry attempts for failed events.
	// Default: 3
	OutboxRetryLimit int

	// ErrorHandler is called when event dispatch fails.
	// Default: logs to stderr
	ErrorHandler func(event PulseEvent, err error)

	// EventFilter allows filtering which events are dispatched.
	// Return false to skip an event.
	// Default: nil (all events are dispatched)
	EventFilter func(event PulseEvent) bool

	// Context provides a base context for async operations.
	// Default: context.Background()
	Context context.Context

	// Logger for debug output. Set to nil to disable logging.
	Logger *log.Logger
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		SyncMode:             SyncModeAsync,
		UseOutbox:            false,
		OutboxTableName:      "pulse_outbox",
		OutboxWorkerInterval: time.Second,
		OutboxBatchSize:      100,
		OutboxRetryLimit:     3,
		ErrorHandler: func(event PulseEvent, err error) {
			log.Printf("[gormpulse] dispatch error: %v (event: %s/%s)", err, event.Table, event.Type)
		},
		Context: context.Background(),
	}
}

// Validate ensures the config is valid.
func (c *Config) Validate() error {
	if c.Dispatcher == nil {
		return ErrNoDispatcher
	}
	return nil
}

// WithDefaults fills in default values for unset fields.
func (c *Config) WithDefaults() *Config {
	defaults := DefaultConfig()

	if c.SyncMode == "" {
		c.SyncMode = defaults.SyncMode
	}
	if c.OutboxTableName == "" {
		c.OutboxTableName = defaults.OutboxTableName
	}
	if c.OutboxWorkerInterval == 0 {
		c.OutboxWorkerInterval = defaults.OutboxWorkerInterval
	}
	if c.OutboxBatchSize == 0 {
		c.OutboxBatchSize = defaults.OutboxBatchSize
	}
	if c.OutboxRetryLimit == 0 {
		c.OutboxRetryLimit = defaults.OutboxRetryLimit
	}
	if c.ErrorHandler == nil {
		c.ErrorHandler = defaults.ErrorHandler
	}
	if c.Context == nil {
		c.Context = defaults.Context
	}

	return c
}
