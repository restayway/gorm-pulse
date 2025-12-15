// Package pulse provides a GORM plugin for emitting domain events on database changes.
// It enables external systems to stay in sync with database state through a clean,
// transaction-aware event system.
package pulse

import (
	"time"
)

// PulseEventType represents the type of database operation that triggered the event.
type PulseEventType string

const (
	// PulseCreate indicates a new record was created.
	PulseCreate PulseEventType = "create"
	// PulseUpdate indicates an existing record was updated.
	PulseUpdate PulseEventType = "update"
	// PulseDelete indicates a record was deleted.
	PulseDelete PulseEventType = "delete"
)

// PulseEvent represents a domain event emitted when a database change occurs.
// This is the core event structure that dispatchers receive and process.
type PulseEvent struct {
	// Type is the kind of operation (create, update, delete).
	Type PulseEventType `json:"type"`

	// Table is the database table name where the change occurred.
	Table string `json:"table"`

	// Model is the Go struct name (useful for debugging and routing).
	Model string `json:"model"`

	// PrimaryKey is the primary key value(s) of the affected record.
	// Can be a single value or composite key.
	PrimaryKey any `json:"primary_key"`

	// Before contains the model state before the operation (nil for create).
	// Note: Capturing "before" state requires additional queries and is optional.
	Before any `json:"before,omitempty"`

	// After contains the model state after the operation (nil for delete).
	After any `json:"after,omitempty"`

	// Document is the Typesense-friendly representation from Pulse().
	Document *PulseDocument `json:"document,omitempty"`

	// OccurredAt is when the event was created.
	OccurredAt time.Time `json:"occurred_at"`
}

// PulseDocument represents a document structure suitable for indexing
// in search engines like Typesense. Models implement Pulse() to return this.
type PulseDocument struct {
	// ID is the unique identifier for the document in the search index.
	ID string `json:"id"`

	// Collection is the target collection/index name.
	Collection string `json:"collection"`

	// Document contains the actual data to be indexed.
	Document map[string]any `json:"document"`
}

// PulseModel is the interface that models must implement to participate
// in event emission. This opt-in approach ensures only explicitly marked
// models trigger events, preventing accidental event floods.
type PulseModel interface {
	// Pulse returns a document representation suitable for external systems.
	// This method is called after successful database operations.
	Pulse() PulseDocument
}

// PulseModelWithPrimaryKey extends PulseModel with explicit primary key access.
// Implement this if your model has a non-standard primary key field.
type PulseModelWithPrimaryKey interface {
	PulseModel
	// PulsePrimaryKey returns the primary key value for this model.
	PulsePrimaryKey() any
}
