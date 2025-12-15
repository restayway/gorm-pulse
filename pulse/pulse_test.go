package pulse

import (
	"context"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestModel is a test model that implements PulseModel.
type TestModel struct {
	ID   string `gorm:"primaryKey"`
	Name string
	Age  int
}

func (m TestModel) Pulse() PulseDocument {
	return PulseDocument{
		ID:         m.ID,
		Collection: "test_models",
		Document: map[string]any{
			"id":   m.ID,
			"name": m.Name,
			"age":  m.Age,
		},
	}
}

// NonPulseModel is a model that does NOT implement PulseModel.
type NonPulseModel struct {
	ID   string `gorm:"primaryKey"`
	Data string
}

// TestModelWithPK implements PulseModelWithPrimaryKey.
type TestModelWithPK struct {
	ID   string `gorm:"primaryKey"`
	Name string
}

func (m TestModelWithPK) Pulse() PulseDocument {
	return PulseDocument{
		ID:         m.ID,
		Collection: "test_pk_models",
		Document: map[string]any{
			"id":   m.ID,
			"name": m.Name,
		},
	}
}

func (m TestModelWithPK) PulsePrimaryKey() any {
	return m.ID
}

// MockDispatcher is a test dispatcher that collects events.
type MockDispatcher struct {
	mu       sync.Mutex
	events   []PulseEvent
	failNext bool
	failErr  error
}

func NewMockDispatcher() *MockDispatcher {
	return &MockDispatcher{
		events: make([]PulseEvent, 0),
	}
}

func (d *MockDispatcher) Dispatch(ctx context.Context, event PulseEvent) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.failNext {
		d.failNext = false
		if d.failErr != nil {
			return d.failErr
		}
		return context.DeadlineExceeded
	}

	d.events = append(d.events, event)
	return nil
}

func (d *MockDispatcher) Events() []PulseEvent {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]PulseEvent{}, d.events...)
}

func (d *MockDispatcher) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = d.events[:0]
}

func (d *MockDispatcher) SetFailNext(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failNext = true
	d.failErr = err
}

// setupTestDB creates a test database with the plugin.
func setupTestDB(t *testing.T, config *Config) (*gorm.DB, *MockDispatcher) {
	t.Helper()

	dispatcher := NewMockDispatcher()
	if config == nil {
		config = &Config{}
	}
	config.Dispatcher = dispatcher

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}

	plugin := New(config)
	if err := db.Use(plugin); err != nil {
		t.Fatalf("failed to use plugin: %v", err)
	}

	if err := db.AutoMigrate(&TestModel{}, &NonPulseModel{}, &TestModelWithPK{}); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}

	return db, dispatcher
}

func TestPluginInitialization(t *testing.T) {
	t.Run("with valid config", func(t *testing.T) {
		db, _ := setupTestDB(t, &Config{SyncMode: SyncModeSync})
		if db == nil {
			t.Fatal("expected db to be non-nil")
		}
	})

	t.Run("without dispatcher", func(t *testing.T) {
		db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			t.Fatalf("failed to connect database: %v", err)
		}

		plugin := New(&Config{})
		err = db.Use(plugin)
		if err != ErrNoDispatcher {
			t.Errorf("expected ErrNoDispatcher, got %v", err)
		}
	})

	t.Run("plugin name", func(t *testing.T) {
		plugin := New(&Config{Dispatcher: NoOpDispatcher{}})
		if plugin.Name() != "gormpulse" {
			t.Errorf("expected plugin name 'gormpulse', got %s", plugin.Name())
		}
	})
}

func TestCreateEvent(t *testing.T) {
	db, dispatcher := setupTestDB(t, &Config{SyncMode: SyncModeSync})

	model := TestModel{ID: "test-1", Name: "Test", Age: 25}
	if err := db.Create(&model).Error; err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	events := dispatcher.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	event := events[0]
	if event.Type != PulseCreate {
		t.Errorf("expected event type %s, got %s", PulseCreate, event.Type)
	}
	if event.Table != "test_models" {
		t.Errorf("expected table 'test_models', got %s", event.Table)
	}
	if event.Document == nil {
		t.Error("expected document to be non-nil")
	}
	if event.Document.Collection != "test_models" {
		t.Errorf("expected collection 'test_models', got %s", event.Document.Collection)
	}
	if event.Document.ID != "test-1" {
		t.Errorf("expected document ID 'test-1', got %s", event.Document.ID)
	}
}

func TestUpdateEvent(t *testing.T) {
	db, dispatcher := setupTestDB(t, &Config{SyncMode: SyncModeSync})

	model := TestModel{ID: "test-1", Name: "Test", Age: 25}
	db.Create(&model)
	dispatcher.Reset()

	if err := db.Model(&model).Update("Name", "Updated").Error; err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	events := dispatcher.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	event := events[0]
	if event.Type != PulseUpdate {
		t.Errorf("expected event type %s, got %s", PulseUpdate, event.Type)
	}
}

func TestDeleteEvent(t *testing.T) {
	db, dispatcher := setupTestDB(t, &Config{SyncMode: SyncModeSync})

	model := TestModel{ID: "test-1", Name: "Test", Age: 25}
	db.Create(&model)
	dispatcher.Reset()

	if err := db.Delete(&model).Error; err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	events := dispatcher.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	event := events[0]
	if event.Type != PulseDelete {
		t.Errorf("expected event type %s, got %s", PulseDelete, event.Type)
	}
}

func TestNonPulseModelIgnored(t *testing.T) {
	db, dispatcher := setupTestDB(t, &Config{SyncMode: SyncModeSync})

	model := NonPulseModel{ID: "test-1", Data: "test"}
	if err := db.Create(&model).Error; err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	events := dispatcher.Events()
	if len(events) != 0 {
		t.Errorf("expected 0 events for non-pulse model, got %d", len(events))
	}
}

func TestBatchCreate(t *testing.T) {
	db, dispatcher := setupTestDB(t, &Config{SyncMode: SyncModeSync})

	models := []TestModel{
		{ID: "batch-1", Name: "One", Age: 1},
		{ID: "batch-2", Name: "Two", Age: 2},
		{ID: "batch-3", Name: "Three", Age: 3},
	}

	if err := db.Create(&models).Error; err != nil {
		t.Fatalf("failed to batch create: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	events := dispatcher.Events()
	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}

	for _, event := range events {
		if event.Type != PulseCreate {
			t.Errorf("expected all events to be create, got %s", event.Type)
		}
	}
}

func TestEventFilter(t *testing.T) {
	filter := func(event PulseEvent) bool {
		return event.Type == PulseCreate
	}

	db, dispatcher := setupTestDB(t, &Config{
		SyncMode:    SyncModeSync,
		EventFilter: filter,
	})

	model := TestModel{ID: "test-1", Name: "Test", Age: 25}
	db.Create(&model)
	db.Model(&model).Update("Name", "Updated")
	db.Delete(&model)

	time.Sleep(10 * time.Millisecond)

	events := dispatcher.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event (filtered), got %d", len(events))
	}

	if events[0].Type != PulseCreate {
		t.Errorf("expected create event, got %s", events[0].Type)
	}
}

func TestAsyncMode(t *testing.T) {
	db, dispatcher := setupTestDB(t, &Config{SyncMode: SyncModeAsync})

	model := TestModel{ID: "async-1", Name: "Async", Age: 30}
	if err := db.Create(&model).Error; err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	events := dispatcher.Events()
	if len(events) != 1 {
		t.Errorf("expected 1 event in async mode, got %d", len(events))
	}
}

func TestSyncMode(t *testing.T) {
	db, dispatcher := setupTestDB(t, &Config{SyncMode: SyncModeSync})

	model := TestModel{ID: "sync-1", Name: "Sync", Age: 30}
	if err := db.Create(&model).Error; err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	events := dispatcher.Events()
	if len(events) != 1 {
		t.Errorf("expected 1 event immediately in sync mode, got %d", len(events))
	}
}

func TestPulseModelWithPrimaryKey(t *testing.T) {
	db, dispatcher := setupTestDB(t, &Config{SyncMode: SyncModeSync})

	db.AutoMigrate(&TestModelWithPK{})

	model := TestModelWithPK{ID: "custom-pk-1", Name: "Custom"}
	if err := db.Create(&model).Error; err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	events := dispatcher.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].PrimaryKey != "custom-pk-1" {
		t.Errorf("expected primary key 'custom-pk-1', got %v", events[0].PrimaryKey)
	}
}

func TestErrorHandler(t *testing.T) {
	var capturedErr error
	var capturedEvent PulseEvent

	dispatcher := NewMockDispatcher()
	dispatcher.SetFailNext(context.DeadlineExceeded)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}

	plugin := New(&Config{
		Dispatcher: dispatcher,
		SyncMode:   SyncModeSync,
		ErrorHandler: func(event PulseEvent, err error) {
			capturedErr = err
			capturedEvent = event
		},
	})

	db.Use(plugin)
	db.AutoMigrate(&TestModel{})

	model := TestModel{ID: "err-1", Name: "Error", Age: 1}
	db.Create(&model)

	if capturedErr == nil {
		t.Error("expected error to be captured")
	}
	if capturedEvent.Type != PulseCreate {
		t.Errorf("expected captured event type to be create, got %s", capturedEvent.Type)
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.SyncMode != SyncModeAsync {
		t.Errorf("expected default sync mode to be async, got %s", config.SyncMode)
	}
	if config.UseOutbox {
		t.Error("expected default UseOutbox to be false")
	}
	if config.OutboxTableName != "pulse_outbox" {
		t.Errorf("expected default outbox table name 'pulse_outbox', got %s", config.OutboxTableName)
	}
	if config.OutboxBatchSize != 100 {
		t.Errorf("expected default batch size 100, got %d", config.OutboxBatchSize)
	}
}

func TestConfigWithDefaults(t *testing.T) {
	config := &Config{
		Dispatcher: NoOpDispatcher{},
	}
	config = config.WithDefaults()

	if config.SyncMode != SyncModeAsync {
		t.Errorf("expected sync mode to be filled with async, got %s", config.SyncMode)
	}
	if config.OutboxTableName != "pulse_outbox" {
		t.Errorf("expected outbox table name to be filled, got %s", config.OutboxTableName)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config := &Config{Dispatcher: NoOpDispatcher{}}
		if err := config.Validate(); err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("missing dispatcher", func(t *testing.T) {
		config := &Config{}
		if err := config.Validate(); err != ErrNoDispatcher {
			t.Errorf("expected ErrNoDispatcher, got %v", err)
		}
	})
}
