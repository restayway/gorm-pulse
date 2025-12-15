package pulse

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestOutboxEntry tests OutboxEntry structure.
func TestOutboxEntry(t *testing.T) {
	now := time.Now()
	entry := OutboxEntry{
		ID:         1,
		EventType:  "create",
		Table:      "users",
		ModelName:  "User",
		PrimaryKey: `"user-1"`,
		Payload:    `{"type":"create"}`,
		Status:     OutboxStatusPending,
		RetryCount: 0,
		CreatedAt:  now,
	}

	if entry.Status != OutboxStatusPending {
		t.Errorf("expected pending status, got %s", entry.Status)
	}
	if entry.RetryCount != 0 {
		t.Errorf("expected 0 retry count, got %d", entry.RetryCount)
	}
}

// TestOutboxStatus tests status constants.
func TestOutboxStatus(t *testing.T) {
	tests := []struct {
		status   OutboxStatus
		expected string
	}{
		{OutboxStatusPending, "pending"},
		{OutboxStatusProcessed, "processed"},
		{OutboxStatusFailed, "failed"},
	}

	for _, tt := range tests {
		if string(tt.status) != tt.expected {
			t.Errorf("expected %s, got %s", tt.expected, tt.status)
		}
	}
}

// TestWriteToOutbox tests writing events to the outbox.
func TestWriteToOutbox(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}

	// Create outbox table using raw SQL for SQLite
	err = db.Exec(`CREATE TABLE IF NOT EXISTS pulse_outbox (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_type VARCHAR(20) NOT NULL,
		table_name VARCHAR(255) NOT NULL,
		model_name VARCHAR(255) NOT NULL,
		primary_key TEXT NOT NULL,
		payload TEXT NOT NULL,
		status VARCHAR(20) NOT NULL DEFAULT 'pending',
		retry_count INTEGER DEFAULT 0,
		last_error TEXT,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		processed_at DATETIME
	)`).Error
	if err != nil {
		t.Fatalf("failed to create outbox table: %v", err)
	}

	event := PulseEvent{
		Type:       PulseCreate,
		Table:      "users",
		Model:      "User",
		PrimaryKey: "user-1",
		Document: &PulseDocument{
			ID:         "user-1",
			Collection: "users",
			Document:   map[string]any{"id": "user-1"},
		},
		OccurredAt: time.Now(),
	}

	err = WriteToOutbox(db, "pulse_outbox", event)
	if err != nil {
		t.Fatalf("failed to write to outbox: %v", err)
	}

	// Verify entry was created
	var entry OutboxEntry
	err = db.Table("pulse_outbox").First(&entry).Error
	if err != nil {
		t.Fatalf("failed to read outbox entry: %v", err)
	}

	if entry.EventType != "create" {
		t.Errorf("expected event type 'create', got %s", entry.EventType)
	}
	if entry.Table != "users" {
		t.Errorf("expected table 'users', got %s", entry.Table)
	}
	if entry.Status != OutboxStatusPending {
		t.Errorf("expected pending status, got %s", entry.Status)
	}
}

// TestOutboxWorker tests the outbox worker lifecycle.
func TestOutboxWorkerLifecycle(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}

	// Create outbox table
	db.Exec(`CREATE TABLE IF NOT EXISTS pulse_outbox (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_type VARCHAR(20) NOT NULL,
		table_name VARCHAR(255) NOT NULL,
		model_name VARCHAR(255) NOT NULL,
		primary_key TEXT NOT NULL,
		payload TEXT NOT NULL,
		status VARCHAR(20) NOT NULL DEFAULT 'pending',
		retry_count INTEGER DEFAULT 0,
		last_error TEXT,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		processed_at DATETIME
	)`)

	config := &Config{
		Dispatcher:           NoOpDispatcher{},
		OutboxTableName:      "pulse_outbox",
		OutboxWorkerInterval: 100 * time.Millisecond,
		OutboxBatchSize:      10,
		OutboxRetryLimit:     3,
	}

	worker := NewOutboxWorker(db, config)

	// Test IsRunning before start
	if worker.IsRunning() {
		t.Error("worker should not be running before Start()")
	}

	// Test Start
	err = worker.Start()
	if err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	if !worker.IsRunning() {
		t.Error("worker should be running after Start()")
	}

	// Test double start
	err = worker.Start()
	if err != ErrWorkerAlreadyRunning {
		t.Errorf("expected ErrWorkerAlreadyRunning, got %v", err)
	}

	// Test Stop
	err = worker.Stop()
	if err != nil {
		t.Fatalf("failed to stop worker: %v", err)
	}

	if worker.IsRunning() {
		t.Error("worker should not be running after Stop()")
	}

	// Test double stop
	err = worker.Stop()
	if err != ErrWorkerNotRunning {
		t.Errorf("expected ErrWorkerNotRunning, got %v", err)
	}
}

// TestOutboxWorkerProcessing tests that the worker processes entries.
func TestOutboxWorkerProcessing(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}

	// Create outbox table
	db.Exec(`CREATE TABLE IF NOT EXISTS pulse_outbox (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_type VARCHAR(20) NOT NULL,
		table_name VARCHAR(255) NOT NULL,
		model_name VARCHAR(255) NOT NULL,
		primary_key TEXT NOT NULL,
		payload TEXT NOT NULL,
		status VARCHAR(20) NOT NULL DEFAULT 'pending',
		retry_count INTEGER DEFAULT 0,
		last_error TEXT,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		processed_at DATETIME
	)`)

	var dispatchCount int32
	dispatcher := DispatcherFunc(func(ctx context.Context, event PulseEvent) error {
		atomic.AddInt32(&dispatchCount, 1)
		return nil
	})

	config := &Config{
		Dispatcher:           dispatcher,
		OutboxTableName:      "pulse_outbox",
		OutboxWorkerInterval: 50 * time.Millisecond,
		OutboxBatchSize:      10,
		OutboxRetryLimit:     3,
		Context:              context.Background(),
	}

	// Insert test entries
	for i := 0; i < 3; i++ {
		payload, _ := json.Marshal(PulseEvent{
			Type:       PulseCreate,
			Table:      "test",
			Model:      "Test",
			PrimaryKey: i,
		})
		db.Exec(`INSERT INTO pulse_outbox (event_type, table_name, model_name, primary_key, payload, status)
			VALUES (?, ?, ?, ?, ?, ?)`, "create", "test", "Test", i, string(payload), "pending")
	}

	worker := NewOutboxWorker(db, config)
	worker.Start()

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	worker.Stop()

	// Check dispatch count
	count := atomic.LoadInt32(&dispatchCount)
	if count != 3 {
		t.Errorf("expected 3 dispatches, got %d", count)
	}

	// Check that entries are processed
	var pendingCount int64
	db.Table("pulse_outbox").Where("status = ?", "pending").Count(&pendingCount)
	if pendingCount != 0 {
		t.Errorf("expected 0 pending entries, got %d", pendingCount)
	}
}

// TestOutboxEntryTableName tests custom table name.
func TestOutboxEntryTableName(t *testing.T) {
	entry := OutboxEntry{}
	if entry.TableName() != "pulse_outbox" {
		t.Errorf("expected default table name 'pulse_outbox', got %s", entry.TableName())
	}
}
