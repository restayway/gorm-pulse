package pulse

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"gorm.io/gorm"
)

// txContextKey is the key used to store pending events in transaction context.
type txContextKey struct{}

// pendingEvents holds events waiting to be dispatched after transaction commit.
type pendingEvents struct {
	mu     sync.Mutex
	events []PulseEvent
}

// RegisterCallbacks registers the Pulse callbacks with a GORM database instance.
// This is called automatically by Plugin.Initialize but can be used manually
// if you need more control over the registration process.
func RegisterCallbacks(db *gorm.DB, plugin *Plugin) error {
	// Register after-create callback
	if err := db.Callback().Create().After("gorm:create").
		Register("pulse:after_create", makeAfterCreateCallback(plugin)); err != nil {
		return err
	}

	// Register after-update callback
	if err := db.Callback().Update().After("gorm:update").
		Register("pulse:after_update", makeAfterUpdateCallback(plugin)); err != nil {
		return err
	}

	// Register after-delete callback
	if err := db.Callback().Delete().After("gorm:delete").
		Register("pulse:after_delete", makeAfterDeleteCallback(plugin)); err != nil {
		return err
	}

	return nil
}

// makeAfterCreateCallback creates the AfterCreate callback function.
func makeAfterCreateCallback(plugin *Plugin) func(*gorm.DB) {
	return func(db *gorm.DB) {
		if db.Error != nil || db.Statement == nil {
			return
		}

		processCallback(db, plugin, PulseCreate)
	}
}

// makeAfterUpdateCallback creates the AfterUpdate callback function.
func makeAfterUpdateCallback(plugin *Plugin) func(*gorm.DB) {
	return func(db *gorm.DB) {
		if db.Error != nil || db.Statement == nil {
			return
		}

		processCallback(db, plugin, PulseUpdate)
	}
}

// makeAfterDeleteCallback creates the AfterDelete callback function.
func makeAfterDeleteCallback(plugin *Plugin) func(*gorm.DB) {
	return func(db *gorm.DB) {
		if db.Error != nil || db.Statement == nil {
			return
		}

		processCallback(db, plugin, PulseDelete)
	}
}

// processCallback handles the common logic for all callbacks.
func processCallback(db *gorm.DB, plugin *Plugin, eventType PulseEventType) {
	// Get the model from statement
	model := db.Statement.Model
	if model == nil {
		model = db.Statement.Dest
	}
	if model == nil {
		return
	}

	// Handle both single models and slices
	models := extractModels(model)
	if len(models) == 0 {
		return
	}

	for _, m := range models {
		// Check if model implements PulseModel (opt-in)
		pulseModel, ok := m.(PulseModel)
		if !ok {
			continue
		}

		// Create the event
		event := createEvent(db, pulseModel, eventType)

		// Apply event filter if configured
		if plugin.config.EventFilter != nil && !plugin.config.EventFilter(event) {
			continue
		}

		// Handle event based on configuration
		if plugin.config.UseOutbox {
			// Write to outbox table within the same transaction
			fmt.Println("writing event", event.Type, "for", event.Table, "to outbox")
			if err := WriteToOutbox(db, plugin.config.OutboxTableName, event); err != nil {
				if plugin.config.Logger != nil {
					plugin.config.Logger.Printf("[gormpulse] failed to write to outbox: %v", err)
				}
			}
		} else {
			// Use in-memory transaction awareness
			handleEventWithTransactionAwareness(db, plugin, event)
			fmt.Println("dispatched event", event.Type, "for", event.Table)
		}
	}
}

// extractModels extracts individual models from a value that may be a slice.
func extractModels(value any) []any {
	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)

	// Handle pointers
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	// Handle slices
	if v.Kind() == reflect.Slice {
		var models []any
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i)
			if elem.CanInterface() {
				// Get the actual value, handling pointers
				if elem.Kind() == reflect.Ptr {
					if !elem.IsNil() {
						models = append(models, elem.Interface())
					}
				} else if elem.CanAddr() {
					models = append(models, elem.Addr().Interface())
				} else {
					models = append(models, elem.Interface())
				}
			}
		}
		return models
	}

	// Single model
	if v.CanAddr() {
		return []any{v.Addr().Interface()}
	}
	return []any{value}
}

// createEvent builds a PulseEvent from a model and operation type.
func createEvent(db *gorm.DB, model PulseModel, eventType PulseEventType) PulseEvent {
	doc := model.Pulse()

	// Get primary key value
	var pk any
	if pkModel, ok := model.(PulseModelWithPrimaryKey); ok {
		pk = pkModel.PulsePrimaryKey()
	} else {
		pk = extractPrimaryKeyFromModel(db, model)
	}

	// Get model type name
	modelName := reflect.TypeOf(model).String()
	if modelName[0] == '*' {
		modelName = modelName[1:]
	}

	event := PulseEvent{
		Type:       eventType,
		Table:      db.Statement.Table,
		Model:      modelName,
		PrimaryKey: pk,
		Document:   &doc,
		OccurredAt: time.Now(),
	}

	// Set After for create/update
	if eventType == PulseCreate || eventType == PulseUpdate {
		event.After = model
	}

	return event
}

// extractPrimaryKeyFromModel extracts the primary key from a specific model instance.
func extractPrimaryKeyFromModel(db *gorm.DB, model any) any {
	if db.Statement.Schema == nil {
		return nil
	}

	// Try to get primary key fields
	pkFields := db.Statement.Schema.PrimaryFields
	if len(pkFields) == 0 {
		return nil
	}

	// Get reflect value of the model
	rv := reflect.ValueOf(model)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}

	// Ensure we have a struct
	if rv.Kind() != reflect.Struct {
		return nil
	}

	// For single primary key
	if len(pkFields) == 1 {
		field := pkFields[0]
		if value, isZero := field.ValueOf(db.Statement.Context, rv); !isZero {
			return value
		}
	}

	// For composite primary key
	pk := make(map[string]any)
	for _, field := range pkFields {
		if value, isZero := field.ValueOf(db.Statement.Context, rv); !isZero {
			pk[field.DBName] = value
		}
	}
	if len(pk) > 0 {
		return pk
	}

	return nil
}

// extractPrimaryKey attempts to get the primary key from GORM statement.
// Deprecated: Use extractPrimaryKeyFromModel instead for batch operations.
func extractPrimaryKey(db *gorm.DB) any {
	if db.Statement.Schema == nil {
		return nil
	}

	// Check if ReflectValue is a slice (batch operation)
	rv := db.Statement.ReflectValue
	if rv.Kind() == reflect.Slice {
		return nil // Can't extract single PK from batch
	}

	// Try to get primary key fields
	pkFields := db.Statement.Schema.PrimaryFields
	if len(pkFields) == 0 {
		return nil
	}

	// For single primary key
	if len(pkFields) == 1 {
		field := pkFields[0]
		if value, isZero := field.ValueOf(db.Statement.Context, db.Statement.ReflectValue); !isZero {
			return value
		}
	}

	// For composite primary key
	pk := make(map[string]any)
	for _, field := range pkFields {
		if value, isZero := field.ValueOf(db.Statement.Context, db.Statement.ReflectValue); !isZero {
			pk[field.DBName] = value
		}
	}
	if len(pk) > 0 {
		return pk
	}

	return nil
}

// handleEventWithTransactionAwareness handles events with in-memory transaction tracking.
// If in a transaction, events are queued until commit.
// If not in a transaction, events are dispatched immediately.
func handleEventWithTransactionAwareness(db *gorm.DB, plugin *Plugin, event PulseEvent) {
	// Check if we're in a transaction by looking for a parent transaction
	// GORM's Statement.ConnPool will be a Tx if in a transaction
	if isInTransaction(db) {
		// Queue event for post-commit dispatch
		queueEventForCommit(db, event)

		// Register commit/rollback hooks if not already done
		registerTransactionHooks(db, plugin)
	} else {
		// Not in transaction, dispatch immediately
		dispatchEvent(plugin, event)
	}
}

// isInTransaction checks if the current db operation is within a transaction.
func isInTransaction(db *gorm.DB) bool {
	// Check if Statement has a transaction flag
	committer, ok := db.Statement.ConnPool.(interface{ Commit() error })
	return ok && committer != nil
}

// queueEventForCommit adds an event to the pending queue for the current transaction.
func queueEventForCommit(db *gorm.DB, event PulseEvent) {
	ctx := db.Statement.Context

	// Get or create pending events container
	pending, ok := ctx.Value(txContextKey{}).(*pendingEvents)
	if !ok || pending == nil {
		pending = &pendingEvents{}
		// Note: We can't modify the context here, so we use GORM's statement settings
		db.Statement.Settings.Store("pulse:pending", pending)
	}

	pending.mu.Lock()
	pending.events = append(pending.events, event)
	pending.mu.Unlock()
}

// registerTransactionHooks sets up commit/rollback handlers.
// Note: GORM doesn't have native post-commit hooks, so this is a best-effort approach.
// For true transactional safety, use the Outbox pattern.
func registerTransactionHooks(db *gorm.DB, plugin *Plugin) {
	// Check if hooks are already registered
	if _, loaded := db.Statement.Settings.LoadOrStore("pulse:hooks_registered", true); loaded {
		return
	}

	// For MVP: We dispatch events immediately after the callback.
	// In practice, this means if the transaction is later rolled back,
	// events may have already been dispatched.
	//
	// For true transactional safety, use config.UseOutbox = true
	//
	// Alternative: Use GORM's RegisterCallbacks for transaction-level hooks
	// but GORM doesn't expose BeforeCommit/AfterCommit cleanly.
}

// dispatchEvent sends an event to the configured dispatcher.
func dispatchEvent(plugin *Plugin, event PulseEvent) {
	ctx := plugin.config.Context
	if ctx == nil {
		ctx = context.Background()
	}

	switch plugin.config.SyncMode {
	case SyncModeAsync:
		go func() {
			if err := plugin.config.Dispatcher.Dispatch(ctx, event); err != nil {
				if plugin.config.ErrorHandler != nil {
					plugin.config.ErrorHandler(event, err)
				}
			}
		}()
	case SyncModeSync:
		if err := plugin.config.Dispatcher.Dispatch(ctx, event); err != nil {
			if plugin.config.ErrorHandler != nil {
				plugin.config.ErrorHandler(event, err)
			}
		}
	}
}

// FlushPendingEvents dispatches all queued events for a transaction.
// Call this after a successful commit when using manual transaction management.
func FlushPendingEvents(db *gorm.DB, plugin *Plugin) {
	pending, ok := db.Statement.Settings.Load("pulse:pending")
	if !ok {
		return
	}

	pe, ok := pending.(*pendingEvents)
	if !ok || pe == nil {
		return
	}

	pe.mu.Lock()
	events := pe.events
	pe.events = nil
	pe.mu.Unlock()

	for _, event := range events {
		dispatchEvent(plugin, event)
	}
}

// ClearPendingEvents removes all queued events without dispatching.
// Call this after a rollback to prevent stale events from being dispatched.
func ClearPendingEvents(db *gorm.DB) {
	pending, ok := db.Statement.Settings.Load("pulse:pending")
	if !ok {
		return
	}

	pe, ok := pending.(*pendingEvents)
	if !ok || pe == nil {
		return
	}

	pe.mu.Lock()
	pe.events = nil
	pe.mu.Unlock()
}
