package nats

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/restayway/gorm-pulse/pulse"
)

func startTestServer(t *testing.T) (*server.Server, string) {
	t.Helper()
	opts := &server.Options{
		Host: "127.0.0.1",
		Port: -1, // Random available port
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create test server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	return ns, ns.ClientURL()
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.SubjectPrefix != "pulse" {
		t.Errorf("expected subject prefix 'pulse', got %s", config.SubjectPrefix)
	}
	if config.ConnectTimeout != 5*time.Second {
		t.Errorf("expected connect timeout 5s, got %v", config.ConnectTimeout)
	}
}

func TestNewDispatcher(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	t.Run("nil config uses defaults", func(t *testing.T) {
		// Need a running server for this
		d, err := NewDispatcher(&Config{URL: url})
		if err != nil {
			t.Fatalf("failed to create dispatcher: %v", err)
		}
		defer d.Close()
		if d.config.SubjectPrefix != "pulse" {
			t.Errorf("expected default subject prefix, got %s", d.config.SubjectPrefix)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		d, err := NewDispatcher(&Config{
			URL:           url,
			SubjectPrefix: "custom",
		})
		if err != nil {
			t.Fatalf("failed to create dispatcher: %v", err)
		}
		defer d.Close()
		if d.config.SubjectPrefix != "custom" {
			t.Errorf("expected 'custom', got %s", d.config.SubjectPrefix)
		}
	})
}

func TestNewDispatcherInvalidURL(t *testing.T) {
	_, err := NewDispatcher(&Config{
		URL: "nats://invalid:4222",
	})
	if err == nil {
		t.Error("expected connection error for invalid URL")
	}
}

func TestNewDispatcherWithExistingConnection(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	// Create a connection first
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	d, err := NewDispatcher(&Config{
		Conn: conn,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	// Should not own the connection
	if d.ownedConn {
		t.Error("dispatcher should not own connection when provided externally")
	}

	// Closing dispatcher should not close the connection
	d.Close()

	if !conn.IsConnected() {
		t.Error("external connection should not be closed")
	}
}

func TestDispatchCreate(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	d, err := NewDispatcher(&Config{
		URL:           url,
		SubjectPrefix: "test",
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	// Subscribe to receive the message - will be test.hotels.create
	receivedCh := make(chan *nats.Msg, 1)
	_, err = d.conn.Subscribe("test.hotels.create", func(msg *nats.Msg) {
		receivedCh <- msg
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

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

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	select {
	case msg := <-receivedCh:
		var received map[string]any
		if err := json.Unmarshal(msg.Data, &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if received["type"] != string(pulse.PulseCreate) {
			t.Errorf("expected PulseCreate type, got %v", received["type"])
		}
		if received["table"] != "hotels" {
			t.Errorf("expected table 'hotels', got %v", received["table"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestDispatchUpdate(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	d, err := NewDispatcher(&Config{
		URL:           url,
		SubjectPrefix: "test",
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	receivedCh := make(chan *nats.Msg, 1)
	_, err = d.conn.Subscribe("test.hotels.update", func(msg *nats.Msg) {
		receivedCh <- msg
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	before := map[string]any{"id": "1", "name": "Old Name"}
	after := map[string]any{"id": "1", "name": "New Name"}

	event := pulse.PulseEvent{
		Type:   pulse.PulseUpdate,
		Table:  "hotels",
		Before: before,
		After:  after,
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	select {
	case msg := <-receivedCh:
		var received map[string]any
		if err := json.Unmarshal(msg.Data, &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if received["type"] != string(pulse.PulseUpdate) {
			t.Errorf("expected PulseUpdate type, got %v", received["type"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestDispatchDelete(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	d, err := NewDispatcher(&Config{
		URL:           url,
		SubjectPrefix: "test",
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	receivedCh := make(chan *nats.Msg, 1)
	_, err = d.conn.Subscribe("test.hotels.delete", func(msg *nats.Msg) {
		receivedCh <- msg
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseDelete,
		Table: "hotels",
		Document: &pulse.PulseDocument{
			ID:         "hotel-1",
			Collection: "hotels",
		},
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	select {
	case msg := <-receivedCh:
		var received map[string]any
		if err := json.Unmarshal(msg.Data, &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if received["type"] != string(pulse.PulseDelete) {
			t.Errorf("expected PulseDelete type, got %v", received["type"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestSubjectMapper(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	d, err := NewDispatcher(&Config{
		URL:           url,
		SubjectPrefix: "base",
		SubjectMapper: func(event pulse.PulseEvent) string {
			return "mapped." + event.Table
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	receivedCh := make(chan *nats.Msg, 1)
	_, err = d.conn.Subscribe("mapped.hotels", func(msg *nats.Msg) {
		receivedCh <- msg
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	select {
	case <-receivedCh:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message on mapped subject")
	}
}

func TestClose(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	d, err := NewDispatcher(&Config{
		URL: url,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	err = d.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	// Connection should be closed
	if d.conn.IsConnected() {
		t.Error("expected connection to be closed")
	}
}

func TestOnErrorCallback(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	var capturedErr error
	var capturedEvent pulse.PulseEvent

	// Create dispatcher with external connection
	conn, _ := nats.Connect(url)
	conn.Close() // Close it to cause errors

	d := &Dispatcher{
		config: &Config{
			SubjectPrefix: "test",
			OnError: func(event pulse.PulseEvent, err error) {
				capturedErr = err
				capturedEvent = event
			},
		},
		conn:      conn,
		ownedConn: false,
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	d.Dispatch(context.Background(), event)

	if capturedErr == nil {
		t.Error("expected error to be captured")
	}
	if capturedEvent.Type != pulse.PulseCreate {
		t.Error("expected event to be captured")
	}
}

func TestIncludeFullModel(t *testing.T) {
	ns, url := startTestServer(t)
	defer ns.Shutdown()

	d, err := NewDispatcher(&Config{
		URL:              url,
		SubjectPrefix:    "test",
		IncludeFullModel: true,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	receivedCh := make(chan *nats.Msg, 1)
	_, err = d.conn.Subscribe("test.hotels.create", func(msg *nats.Msg) {
		receivedCh <- msg
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
		After: map[string]any{
			"id":    "hotel-1",
			"name":  "Full Model Hotel",
			"stars": 5,
		},
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	select {
	case msg := <-receivedCh:
		var received map[string]any
		if err := json.Unmarshal(msg.Data, &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		// With IncludeFullModel, Data field should be included
		if received["data"] == nil {
			t.Error("expected Data field to be included")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}
