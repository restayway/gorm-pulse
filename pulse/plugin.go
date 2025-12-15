package pulse

import (
	"gorm.io/gorm"
)

// Plugin is the main GORM plugin that enables domain event emission.
// It hooks into GORM's lifecycle callbacks and dispatches events
// when models that implement PulseModel are created, updated, or deleted.
type Plugin struct {
	config       *Config
	outboxWorker *OutboxWorker
}

// New creates a new Pulse plugin with the given configuration.
// The dispatcher is required; other config options have sensible defaults.
//
// Example:
//
//	plugin := pulse.New(&pulse.Config{
//	    Dispatcher: myDispatcher,
//	    SyncMode:   pulse.SyncModeAsync,
//	})
//	db.Use(plugin)
func New(config *Config) *Plugin {
	if config == nil {
		config = DefaultConfig()
	}
	config = config.WithDefaults()

	return &Plugin{
		config: config,
	}
}

// Name returns the plugin name for GORM registration.
func (p *Plugin) Name() string {
	return "gormpulse"
}

// Initialize implements gorm.Plugin interface.
// It registers the necessary callbacks with GORM.
func (p *Plugin) Initialize(db *gorm.DB) error {
	// Validate configuration
	if err := p.config.Validate(); err != nil {
		return err
	}

	// Register callbacks
	if err := RegisterCallbacks(db, p); err != nil {
		return err
	}

	if p.config.Logger != nil {
		p.config.Logger.Printf("[gormpulse] plugin initialized (mode: %s, outbox: %v)",
			p.config.SyncMode, p.config.UseOutbox)
	}

	return nil
}

// AutoMigrate creates the outbox table if UseOutbox is enabled.
// Call this after db.Use(plugin) if you're using the outbox pattern.
func (p *Plugin) AutoMigrate(db *gorm.DB) error {
	if !p.config.UseOutbox {
		return nil
	}

	// Create the outbox table with the configured name
	sql := `CREATE TABLE IF NOT EXISTS ` + p.config.OutboxTableName + ` (
		id BIGSERIAL PRIMARY KEY,
		event_type VARCHAR(20) NOT NULL,
		table_name VARCHAR(255) NOT NULL,
		model_name VARCHAR(255) NOT NULL,
		primary_key TEXT NOT NULL,
		payload TEXT NOT NULL,
		status VARCHAR(20) NOT NULL DEFAULT 'pending',
		retry_count INTEGER DEFAULT 0,
		last_error TEXT,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		processed_at TIMESTAMP
	)`

	if err := db.Exec(sql).Error; err != nil {
		// Try MySQL/SQLite compatible syntax
		sql = `CREATE TABLE IF NOT EXISTS ` + p.config.OutboxTableName + ` (
			id INTEGER PRIMARY KEY AUTO_INCREMENT,
			event_type VARCHAR(20) NOT NULL,
			table_name VARCHAR(255) NOT NULL,
			model_name VARCHAR(255) NOT NULL,
			primary_key TEXT NOT NULL,
			payload TEXT NOT NULL,
			status VARCHAR(20) NOT NULL DEFAULT 'pending',
			retry_count INTEGER DEFAULT 0,
			last_error TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			processed_at TIMESTAMP NULL
		)`
		if err := db.Exec(sql).Error; err != nil {
			// Final fallback for SQLite
			sql = `CREATE TABLE IF NOT EXISTS ` + p.config.OutboxTableName + ` (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				event_type TEXT NOT NULL,
				table_name TEXT NOT NULL,
				model_name TEXT NOT NULL,
				primary_key TEXT NOT NULL,
				payload TEXT NOT NULL,
				status TEXT NOT NULL DEFAULT 'pending',
				retry_count INTEGER DEFAULT 0,
				last_error TEXT,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
				processed_at DATETIME
			)`
			if err := db.Exec(sql).Error; err != nil {
				return err
			}
		}
	}

	// Create indexes
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_` + p.config.OutboxTableName + `_status ON ` + p.config.OutboxTableName + ` (status)`,
		`CREATE INDEX IF NOT EXISTS idx_` + p.config.OutboxTableName + `_created ON ` + p.config.OutboxTableName + ` (created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_` + p.config.OutboxTableName + `_event_type ON ` + p.config.OutboxTableName + ` (event_type)`,
	}

	for _, idx := range indexes {
		// Ignore index creation errors (syntax varies by database)
		db.Exec(idx)
	}

	if p.config.Logger != nil {
		p.config.Logger.Printf("[gormpulse] outbox table '%s' created", p.config.OutboxTableName)
	}

	return nil
}

// StartOutboxWorker starts the background worker that processes outbox entries.
// Only call this if UseOutbox is enabled.
func (p *Plugin) StartOutboxWorker(db *gorm.DB) error {
	if !p.config.UseOutbox {
		return nil
	}

	p.outboxWorker = NewOutboxWorker(db, p.config)
	return p.outboxWorker.Start()
}

// StopOutboxWorker gracefully stops the background worker.
func (p *Plugin) StopOutboxWorker() error {
	if p.outboxWorker == nil {
		return ErrWorkerNotRunning
	}
	return p.outboxWorker.Stop()
}

// GetOutboxWorker returns the outbox worker instance.
// Returns nil if outbox is not enabled or worker hasn't been started.
func (p *Plugin) GetOutboxWorker() *OutboxWorker {
	return p.outboxWorker
}

// Config returns the plugin configuration.
func (p *Plugin) Config() *Config {
	return p.config
}

// WithTransaction provides a helper for manual transaction management
// with proper event handling. Use this when you need explicit control
// over when events are dispatched.
//
// Example:
//
//	err := plugin.WithTransaction(db, func(tx *gorm.DB) error {
//	    // Your transactional operations here
//	    return tx.Create(&model).Error
//	})
func (p *Plugin) WithTransaction(db *gorm.DB, fn func(tx *gorm.DB) error) error {
	return db.Transaction(func(tx *gorm.DB) error {
		err := fn(tx)
		if err != nil {
			// Transaction will be rolled back, clear any pending events
			ClearPendingEvents(tx)
			return err
		}
		// Transaction will be committed, flush events
		// Note: In outbox mode, events are already in the table
		if !p.config.UseOutbox {
			FlushPendingEvents(tx, p)
		}
		return nil
	})
}
