package pulse

import (
	"testing"
	"time"
)

// TestPulseEventType tests event type constants.
func TestPulseEventType(t *testing.T) {
	tests := []struct {
		eventType PulseEventType
		expected  string
	}{
		{PulseCreate, "create"},
		{PulseUpdate, "update"},
		{PulseDelete, "delete"},
	}

	for _, tt := range tests {
		if string(tt.eventType) != tt.expected {
			t.Errorf("expected %s, got %s", tt.expected, tt.eventType)
		}
	}
}

// TestPulseEvent tests event structure.
func TestPulseEvent(t *testing.T) {
	now := time.Now()
	doc := &PulseDocument{
		ID:         "doc-1",
		Collection: "users",
		Document: map[string]any{
			"id":   "doc-1",
			"name": "Test",
		},
	}

	event := PulseEvent{
		Type:       PulseCreate,
		Table:      "users",
		Model:      "User",
		PrimaryKey: "doc-1",
		Document:   doc,
		OccurredAt: now,
	}

	if event.Type != PulseCreate {
		t.Errorf("expected type create, got %s", event.Type)
	}
	if event.Table != "users" {
		t.Errorf("expected table 'users', got %s", event.Table)
	}
	if event.Model != "User" {
		t.Errorf("expected model 'User', got %s", event.Model)
	}
	if event.PrimaryKey != "doc-1" {
		t.Errorf("expected primary key 'doc-1', got %v", event.PrimaryKey)
	}
	if event.Document == nil {
		t.Error("expected document to be non-nil")
	}
	if event.Document.ID != "doc-1" {
		t.Errorf("expected document ID 'doc-1', got %s", event.Document.ID)
	}
}

// TestPulseDocument tests document structure.
func TestPulseDocument(t *testing.T) {
	doc := PulseDocument{
		ID:         "test-id",
		Collection: "products",
		Document: map[string]any{
			"id":    "test-id",
			"name":  "Widget",
			"price": 19.99,
		},
	}

	if doc.ID != "test-id" {
		t.Errorf("expected ID 'test-id', got %s", doc.ID)
	}
	if doc.Collection != "products" {
		t.Errorf("expected collection 'products', got %s", doc.Collection)
	}
	if doc.Document["name"] != "Widget" {
		t.Errorf("expected document name 'Widget', got %v", doc.Document["name"])
	}
	if doc.Document["price"] != 19.99 {
		t.Errorf("expected document price 19.99, got %v", doc.Document["price"])
	}
}

// TestPulseEventBeforeAfter tests Before/After fields.
func TestPulseEventBeforeAfter(t *testing.T) {
	t.Run("create event has After only", func(t *testing.T) {
		event := PulseEvent{
			Type:   PulseCreate,
			Before: nil,
			After:  map[string]any{"id": "1"},
		}

		if event.Before != nil {
			t.Error("expected Before to be nil for create event")
		}
		if event.After == nil {
			t.Error("expected After to be non-nil for create event")
		}
	})

	t.Run("update event can have both", func(t *testing.T) {
		event := PulseEvent{
			Type:   PulseUpdate,
			Before: map[string]any{"name": "old"},
			After:  map[string]any{"name": "new"},
		}

		if event.Before == nil {
			t.Error("expected Before to be non-nil for update event")
		}
		if event.After == nil {
			t.Error("expected After to be non-nil for update event")
		}
	})

	t.Run("delete event has Before only", func(t *testing.T) {
		event := PulseEvent{
			Type:   PulseDelete,
			Before: map[string]any{"id": "1"},
			After:  nil,
		}

		if event.Before == nil {
			t.Error("expected Before to be non-nil for delete event")
		}
		if event.After != nil {
			t.Error("expected After to be nil for delete event")
		}
	})
}

// TestPulseEventOccurredAt tests timestamp handling.
func TestPulseEventOccurredAt(t *testing.T) {
	before := time.Now()
	event := PulseEvent{
		Type:       PulseCreate,
		OccurredAt: time.Now(),
	}
	after := time.Now()

	if event.OccurredAt.Before(before) {
		t.Error("OccurredAt should not be before creation time")
	}
	if event.OccurredAt.After(after) {
		t.Error("OccurredAt should not be after current time")
	}
}

// TestPulseDocumentEmptyDocument tests empty document handling.
func TestPulseDocumentEmptyDocument(t *testing.T) {
	doc := PulseDocument{
		ID:         "empty",
		Collection: "test",
		Document:   map[string]any{},
	}

	if len(doc.Document) != 0 {
		t.Errorf("expected empty document, got %d fields", len(doc.Document))
	}
}

// TestPulseDocumentNilDocument tests nil document handling.
func TestPulseDocumentNilDocument(t *testing.T) {
	doc := PulseDocument{
		ID:         "nil",
		Collection: "test",
		Document:   nil,
	}

	if doc.Document != nil {
		t.Error("expected nil document")
	}
}
