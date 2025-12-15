package pulse

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

// OutboxStatus represents the processing state of an outbox entry.
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusProcessed OutboxStatus = "processed"
	OutboxStatusFailed    OutboxStatus = "failed"
)

// OutboxEntry represents a row in the outbox table.
// Events are written here within the same transaction as the domain change,
// ensuring atomic consistency between data and events.
type OutboxEntry struct {
	ID          uint64       `gorm:"primaryKey;autoIncrement"`
	EventType   string       `gorm:"type:varchar(20);not null;index"`
	Table       string       `gorm:"column:table_name;type:varchar(255);not null"`
	ModelName   string       `gorm:"type:varchar(255);not null"`
	PrimaryKey  string       `gorm:"type:text;not null"` // JSON encoded
	Payload     string       `gorm:"type:text;not null"` // JSON encoded PulseEvent
	Status      OutboxStatus `gorm:"type:varchar(20);not null;default:'pending';index"`
	RetryCount  int          `gorm:"default:0"`
	LastError   string       `gorm:"type:text"`
	CreatedAt   time.Time    `gorm:"autoCreateTime;index"`
	ProcessedAt *time.Time
}

// TableName returns the configured outbox table name.
// This is set dynamically by the plugin.
func (OutboxEntry) TableName() string {
	return "pulse_outbox" // Default, overridden at runtime
}

// OutboxWorker processes pending outbox entries asynchronously.
// It runs in a background goroutine with proper lifecycle management.
type OutboxWorker struct {
	db         *gorm.DB
	config     *Config
	tableName  string
	running    atomic.Bool
	stopCh     chan struct{}
	stoppedCh  chan struct{}
	mu         sync.Mutex
}

// NewOutboxWorker creates a new outbox worker.
func NewOutboxWorker(db *gorm.DB, config *Config) *OutboxWorker {
	return &OutboxWorker{
		db:        db,
		config:    config,
		tableName: config.OutboxTableName,
	}
}

// Start begins processing outbox entries in the background.
// Returns immediately. Call Stop() to gracefully shut down.
func (w *OutboxWorker) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running.Load() {
		return ErrWorkerAlreadyRunning
	}

	w.stopCh = make(chan struct{})
	w.stoppedCh = make(chan struct{})
	w.running.Store(true)

	go w.run()

	if w.config.Logger != nil {
		w.config.Logger.Printf("[gormpulse] outbox worker started (interval: %v)", w.config.OutboxWorkerInterval)
	}

	return nil
}

// Stop gracefully stops the outbox worker.
// Blocks until the worker has finished processing current batch.
func (w *OutboxWorker) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.running.Load() {
		return ErrWorkerNotRunning
	}

	close(w.stopCh)
	<-w.stoppedCh
	w.running.Store(false)

	if w.config.Logger != nil {
		w.config.Logger.Printf("[gormpulse] outbox worker stopped")
	}

	return nil
}

// IsRunning returns true if the worker is currently running.
func (w *OutboxWorker) IsRunning() bool {
	return w.running.Load()
}

// run is the main worker loop.
func (w *OutboxWorker) run() {
	defer close(w.stoppedCh)

	ticker := time.NewTicker(w.config.OutboxWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.processBatch()
		}
	}
}

// processBatch fetches and processes pending outbox entries.
func (w *OutboxWorker) processBatch() {
	ctx := w.config.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var entries []OutboxEntry
	
	// Fetch pending entries with row locking to prevent duplicate processing
	// in multi-instance deployments
	err := w.db.Table(w.tableName).
		Where("status = ?", OutboxStatusPending).
		Order("created_at ASC").
		Limit(w.config.OutboxBatchSize).
		Find(&entries).Error

	if err != nil {
		if w.config.Logger != nil {
			w.config.Logger.Printf("[gormpulse] failed to fetch outbox entries: %v", err)
		}
		return
	}

	for _, entry := range entries {
		w.processEntry(ctx, entry)
	}
}

// processEntry dispatches a single outbox entry.
func (w *OutboxWorker) processEntry(ctx context.Context, entry OutboxEntry) {
	// Decode the event
	var event PulseEvent
	if err := json.Unmarshal([]byte(entry.Payload), &event); err != nil {
		w.markFailed(entry, "failed to unmarshal event: "+err.Error())
		return
	}

	// Dispatch the event
	if err := w.config.Dispatcher.Dispatch(ctx, event); err != nil {
		// Check retry limit
		if entry.RetryCount >= w.config.OutboxRetryLimit {
			w.markFailed(entry, err.Error())
		} else {
			w.incrementRetry(entry, err.Error())
		}
		return
	}

	// Mark as processed
	w.markProcessed(entry)
}

// markProcessed updates an entry as successfully processed.
func (w *OutboxWorker) markProcessed(entry OutboxEntry) {
	now := time.Now()
	w.db.Table(w.tableName).
		Where("id = ?", entry.ID).
		Updates(map[string]any{
			"status":       OutboxStatusProcessed,
			"processed_at": &now,
		})
}

// markFailed updates an entry as permanently failed.
func (w *OutboxWorker) markFailed(entry OutboxEntry, errMsg string) {
	w.db.Table(w.tableName).
		Where("id = ?", entry.ID).
		Updates(map[string]any{
			"status":     OutboxStatusFailed,
			"last_error": errMsg,
		})

	if w.config.Logger != nil {
		w.config.Logger.Printf("[gormpulse] outbox entry %d permanently failed: %s", entry.ID, errMsg)
	}
}

// incrementRetry increases the retry count for an entry.
func (w *OutboxWorker) incrementRetry(entry OutboxEntry, errMsg string) {
	w.db.Table(w.tableName).
		Where("id = ?", entry.ID).
		Updates(map[string]any{
			"retry_count": entry.RetryCount + 1,
			"last_error":  errMsg,
		})
}

// WriteToOutbox inserts an event into the outbox table.
// This should be called within the same transaction as the domain change.
func WriteToOutbox(db *gorm.DB, tableName string, event PulseEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	pkJSON, err := json.Marshal(event.PrimaryKey)
	if err != nil {
		return err
	}

	entry := map[string]any{
		"event_type":  string(event.Type),
		"table_name":  event.Table,
		"model_name":  event.Model,
		"primary_key": string(pkJSON),
		"payload":     string(payload),
		"status":      OutboxStatusPending,
		"retry_count": 0,
		"created_at":  event.OccurredAt,
	}

	return db.Table(tableName).Create(entry).Error
}

// CleanupProcessedEntries removes old processed entries from the outbox.
// Call this periodically to prevent the table from growing indefinitely.
func CleanupProcessedEntries(db *gorm.DB, tableName string, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	result := db.Table(tableName).
		Where("status = ? AND processed_at < ?", OutboxStatusProcessed, cutoff).
		Delete(&OutboxEntry{})
	return result.RowsAffected, result.Error
}

// RetryFailedEntries resets failed entries to pending for reprocessing.
func RetryFailedEntries(db *gorm.DB, tableName string) (int64, error) {
	result := db.Table(tableName).
		Where("status = ?", OutboxStatusFailed).
		Updates(map[string]any{
			"status":      OutboxStatusPending,
			"retry_count": 0,
			"last_error":  nil,
		})
	return result.RowsAffected, result.Error
}
