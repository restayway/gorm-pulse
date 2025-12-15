package typesense

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/restayway/gorm-pulse/pulse"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Timeout != 10*time.Second {
		t.Errorf("expected timeout 10s, got %v", config.Timeout)
	}
	if config.RetryCount != 3 {
		t.Errorf("expected retry count 3, got %d", config.RetryCount)
	}
	if config.RetryDelay != 100*time.Millisecond {
		t.Errorf("expected retry delay 100ms, got %v", config.RetryDelay)
	}
}

func TestNewDispatcher(t *testing.T) {
	t.Run("with nil config", func(t *testing.T) {
		d := NewDispatcher(nil)
		if d == nil {
			t.Fatal("expected non-nil dispatcher")
		}
		if d.config.Timeout != 10*time.Second {
			t.Errorf("expected default timeout, got %v", d.config.Timeout)
		}
	})

	t.Run("with custom config", func(t *testing.T) {
		config := &Config{
			Host:       "http://localhost:8108",
			APIKey:     "xyz",
			Timeout:    5 * time.Second,
			RetryCount: 5,
		}
		d := NewDispatcher(config)
		if d.config.Timeout != 5*time.Second {
			t.Errorf("expected 5s timeout, got %v", d.config.Timeout)
		}
		if d.config.RetryCount != 5 {
			t.Errorf("expected 5 retries, got %d", d.config.RetryCount)
		}
	})

	t.Run("with custom http client", func(t *testing.T) {
		customClient := &http.Client{Timeout: 1 * time.Second}
		config := &Config{
			HTTPClient: customClient,
		}
		d := NewDispatcher(config)
		if d.client != customClient {
			t.Error("expected custom client to be used")
		}
	})
}

func TestDispatchCreate(t *testing.T) {
	var receivedMethod string
	var receivedPath string
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedPath = r.URL.Path
		json.NewDecoder(r.Body).Decode(&receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(&Config{
		Host:   server.URL,
		APIKey: "test-key",
	})

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
		Document: &pulse.PulseDocument{
			ID:         "hotel-1",
			Collection: "hotels",
			Document: map[string]any{
				"id":   "hotel-1",
				"name": "Test Hotel",
			},
		},
	}

	err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedMethod != http.MethodPost {
		t.Errorf("expected POST method, got %s", receivedMethod)
	}
	if receivedPath != "/collections/hotels/documents" {
		t.Errorf("expected path '/collections/hotels/documents', got %s", receivedPath)
	}
	if receivedBody["name"] != "Test Hotel" {
		t.Errorf("expected name 'Test Hotel', got %v", receivedBody["name"])
	}
}

func TestDispatchDelete(t *testing.T) {
	var receivedMethod string
	var receivedPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(&Config{
		Host:   server.URL,
		APIKey: "test-key",
	})

	event := pulse.PulseEvent{
		Type:  pulse.PulseDelete,
		Table: "hotels",
		Document: &pulse.PulseDocument{
			ID:         "hotel-1",
			Collection: "hotels",
		},
	}

	err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedMethod != http.MethodDelete {
		t.Errorf("expected DELETE method, got %s", receivedMethod)
	}
	if receivedPath != "/collections/hotels/documents/hotel-1" {
		t.Errorf("expected delete path with doc ID, got %s", receivedPath)
	}
}

func TestDispatchNoDocument(t *testing.T) {
	d := NewDispatcher(&Config{
		Host:   "http://localhost:8108",
		APIKey: "test-key",
	})

	event := pulse.PulseEvent{
		Type:     pulse.PulseCreate,
		Table:    "hotels",
		Document: nil,
	}

	err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Errorf("expected no error for nil document, got %v", err)
	}
}

func TestDispatchEmptyCollection(t *testing.T) {
	d := NewDispatcher(&Config{
		Host:   "http://localhost:8108",
		APIKey: "test-key",
	})

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
		Document: &pulse.PulseDocument{
			ID:         "hotel-1",
			Collection: "",
		},
	}

	err := d.Dispatch(context.Background(), event)
	if err == nil {
		t.Error("expected error for empty collection")
	}
}

func TestDispatchUpsertWithoutID(t *testing.T) {
	d := NewDispatcher(&Config{
		Host:   "http://localhost:8108",
		APIKey: "test-key",
	})

	// Create without ID should fail
	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
		Document: &pulse.PulseDocument{
			ID:         "", // Empty ID
			Collection: "hotels",
			Document: map[string]any{
				"name": "Test Hotel",
			},
		},
	}

	err := d.Dispatch(context.Background(), event)
	if err == nil {
		t.Error("expected error for create without ID")
	}
	if err.Error() != "typesense: document ID is required for upsert (create/update)" {
		t.Errorf("unexpected error message: %v", err)
	}

	// Update without ID should also fail
	event.Type = pulse.PulseUpdate
	err = d.Dispatch(context.Background(), event)
	if err == nil {
		t.Error("expected error for update without ID")
	}
}

func TestCollectionMapper(t *testing.T) {
	var receivedPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(&Config{
		Host:   server.URL,
		APIKey: "test-key",
		CollectionMapper: func(event pulse.PulseEvent) string {
			return "custom_" + event.Document.Collection
		},
	})

	event := pulse.PulseEvent{
		Type: pulse.PulseCreate,
		Document: &pulse.PulseDocument{
			ID:         "1",
			Collection: "hotels",
			Document:   map[string]any{"id": "1"},
		},
	}

	d.Dispatch(context.Background(), event)

	if receivedPath != "/collections/custom_hotels/documents" {
		t.Errorf("expected custom collection path, got %s", receivedPath)
	}
}

func TestDocumentTransformer(t *testing.T) {
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(&Config{
		Host:   server.URL,
		APIKey: "test-key",
		DocumentTransformer: func(event pulse.PulseEvent, doc map[string]any) map[string]any {
			doc["transformed"] = true
			return doc
		},
	})

	event := pulse.PulseEvent{
		Type: pulse.PulseCreate,
		Document: &pulse.PulseDocument{
			ID:         "1",
			Collection: "hotels",
			Document:   map[string]any{"id": "1"},
		},
	}

	d.Dispatch(context.Background(), event)

	if receivedBody["transformed"] != true {
		t.Error("expected document to be transformed")
	}
}

func TestRetryLogic(t *testing.T) {
	attempts := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(&Config{
		Host:       server.URL,
		APIKey:     "test-key",
		RetryCount: 3,
		RetryDelay: 1 * time.Millisecond,
	})

	event := pulse.PulseEvent{
		Type: pulse.PulseCreate,
		Document: &pulse.PulseDocument{
			ID:         "1",
			Collection: "hotels",
			Document:   map[string]any{"id": "1"},
		},
	}

	err := d.Dispatch(context.Background(), event)
	if err != nil {
		t.Errorf("expected success after retries, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestNoRetryOn4xx(t *testing.T) {
	attempts := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	d := NewDispatcher(&Config{
		Host:       server.URL,
		APIKey:     "test-key",
		RetryCount: 3,
		RetryDelay: 1 * time.Millisecond,
	})

	event := pulse.PulseEvent{
		Type: pulse.PulseCreate,
		Document: &pulse.PulseDocument{
			ID:         "1",
			Collection: "hotels",
			Document:   map[string]any{"id": "1"},
		},
	}

	err := d.Dispatch(context.Background(), event)
	if err == nil {
		t.Error("expected error on 400")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt (no retry on 4xx), got %d", attempts)
	}
}

func TestOnErrorCallback(t *testing.T) {
	var capturedErr error
	var capturedEvent pulse.PulseEvent

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	d := NewDispatcher(&Config{
		Host:   server.URL,
		APIKey: "test-key",
		OnError: func(event pulse.PulseEvent, err error) {
			capturedErr = err
			capturedEvent = event
		},
	})

	event := pulse.PulseEvent{
		Type: pulse.PulseCreate,
		Document: &pulse.PulseDocument{
			ID:         "1",
			Collection: "hotels",
			Document:   map[string]any{"id": "1"},
		},
	}

	d.Dispatch(context.Background(), event)

	if capturedErr == nil {
		t.Error("expected error to be captured")
	}
	if capturedEvent.Type != pulse.PulseCreate {
		t.Error("expected event to be captured")
	}
}

func TestDeleteWithoutID(t *testing.T) {
	d := NewDispatcher(&Config{
		Host:   "http://localhost:8108",
		APIKey: "test-key",
	})

	event := pulse.PulseEvent{
		Type: pulse.PulseDelete,
		Document: &pulse.PulseDocument{
			ID:         "",
			Collection: "hotels",
		},
	}

	err := d.Dispatch(context.Background(), event)
	if err == nil {
		t.Error("expected error for delete without ID")
	}
}
