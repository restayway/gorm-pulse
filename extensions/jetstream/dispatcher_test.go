package jetstream

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/restayway/gorm-pulse/pulse"
)

func startTestServer(t *testing.T) (*server.Server, string) {
	t.Helper()
	// Use a non-TempDir path to avoid cleanup issues with JetStream store
	storeDir := t.TempDir()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // Random available port
		JetStream: true,
		StoreDir:  storeDir,
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create test server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	// Register cleanup to ensure proper shutdown
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns, ns.ClientURL()
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.SubjectPrefix != "pulse" {
		t.Errorf("expected subject prefix 'pulse', got %s", config.SubjectPrefix)
	}
	if config.StreamName != "PULSE" {
		t.Errorf("expected stream 'PULSE', got %s", config.StreamName)
	}
	if config.CreateStream != true {
		t.Error("expected CreateStream to be true by default")
	}
}

func TestNewDispatcher(t *testing.T) {
	_, url := startTestServer(t)

	t.Run("nil config uses defaults", func(t *testing.T) {
		d, err := NewDispatcher(&Config{URL: url})
		if err != nil {
			t.Fatalf("failed to create dispatcher: %v", err)
		}
		defer d.Close()
		if d.config.SubjectPrefix != "pulse" {
			t.Errorf("expected default subject prefix, got %s", d.config.SubjectPrefix)
		}
		if d.config.StreamName != "PULSE" {
			t.Errorf("expected default stream name, got %s", d.config.StreamName)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		d, err := NewDispatcher(&Config{
			URL:           url,
			SubjectPrefix: "custom",
			StreamName:    "CUSTOM",
		})
		if err != nil {
			t.Fatalf("failed to create dispatcher: %v", err)
		}
		defer d.Close()
		if d.config.SubjectPrefix != "custom" {
			t.Errorf("expected 'custom', got %s", d.config.SubjectPrefix)
		}
		if d.config.StreamName != "CUSTOM" {
			t.Errorf("expected 'CUSTOM', got %s", d.config.StreamName)
		}
	})
}

func TestNewDispatcherInvalidURL(t *testing.T) {
	_, err := NewDispatcher(&Config{
		URL: "nats://invalid:4222",
	})
	if err == nil {
		t.Error("expected connection error")
	}
}

func TestNewDispatcherWithExistingConnection(t *testing.T) {
	_, url := startTestServer(t)

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
	defer d.Close()

	if d.conn == nil {
		t.Error("expected connection to be established")
	}
	if d.js == nil {
		t.Error("expected JetStream context to be established")
	}
}

func TestDispatchCreate(t *testing.T) {
	_, url := startTestServer(t)

	d, err := NewDispatcher(&Config{
		URL:           url,
		SubjectPrefix: "test",
		StreamName:    "TEST",
		CreateStream:  true,
		StreamConfig: &jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	// Create a consumer to receive messages
	cons, err := d.js.CreateOrUpdateConsumer(context.Background(), "TEST", jetstream.ConsumerConfig{
		Durable:       "test-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "test.hotels.create",
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
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

	// Fetch the message
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("failed to fetch: %v", err)
	}

	for msg := range msgs.Messages() {
		var received map[string]any
		if err := json.Unmarshal(msg.Data(), &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if received["type"] != string(pulse.PulseCreate) {
			t.Errorf("expected PulseCreate type, got %v", received["type"])
		}
		if received["table"] != "hotels" {
			t.Errorf("expected table 'hotels', got %v", received["table"])
		}
		msg.Ack()
	}
}

func TestDispatchUpdate(t *testing.T) {
	_, url := startTestServer(t)

	d, err := NewDispatcher(&Config{
		URL:           url,
		SubjectPrefix: "test",
		StreamName:    "TEST",
		CreateStream:  true,
		StreamConfig: &jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	cons, err := d.js.CreateOrUpdateConsumer(context.Background(), "TEST", jetstream.ConsumerConfig{
		Durable:       "test-consumer-update",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "test.hotels.update",
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
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

	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("failed to fetch: %v", err)
	}

	for msg := range msgs.Messages() {
		var received map[string]any
		if err := json.Unmarshal(msg.Data(), &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if received["type"] != string(pulse.PulseUpdate) {
			t.Errorf("expected PulseUpdate type, got %v", received["type"])
		}
		msg.Ack()
	}
}

func TestSubjectMapper(t *testing.T) {
	_, url := startTestServer(t)

	d, err := NewDispatcher(&Config{
		URL:           url,
		SubjectPrefix: "base",
		StreamName:    "TEST",
		CreateStream:  true,
		StreamConfig: &jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"base.>", "mapped.>"},
		},
		SubjectMapper: func(event pulse.PulseEvent) string {
			return "mapped." + event.Table
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	cons, err := d.js.CreateOrUpdateConsumer(context.Background(), "TEST", jetstream.ConsumerConfig{
		Durable:       "test-consumer-mapped",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "mapped.hotels",
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("failed to fetch: %v", err)
	}

	count := 0
	for msg := range msgs.Messages() {
		count++
		msg.Ack()
	}
	if count == 0 {
		t.Error("expected to receive message on mapped subject")
	}
}

func TestClose(t *testing.T) {
	_, url := startTestServer(t)

	d, err := NewDispatcher(&Config{
		URL:        url,
		StreamName: "TEST",
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	err = d.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}
}

func TestStreamCreation(t *testing.T) {
	_, url := startTestServer(t)

	streamName := "CUSTOM_STREAM"

	d, err := NewDispatcher(&Config{
		URL:          url,
		StreamName:   streamName,
		CreateStream: true,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	// Verify stream was created
	nc, _ := nats.Connect(url)
	defer nc.Close()

	js, _ := jetstream.New(nc)
	stream, err := js.Stream(context.Background(), streamName)
	if err != nil {
		t.Fatalf("stream not created: %v", err)
	}

	info, _ := stream.Info(context.Background())
	if info.Config.Name != streamName {
		t.Errorf("expected stream name %s, got %s", streamName, info.Config.Name)
	}
}

func TestOnPublishAck(t *testing.T) {
	_, url := startTestServer(t)

	var ackReceived bool

	d, err := NewDispatcher(&Config{
		URL:           url,
		SubjectPrefix: "pulse",
		StreamName:    "TEST",
		CreateStream:  true,
		StreamConfig: &jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"pulse.>"},
		},
		OnPublishAck: func(event pulse.PulseEvent, ack *jetstream.PubAck) {
			ackReceived = true
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	if !ackReceived {
		t.Error("expected OnPublishAck callback to be called")
	}
}

func TestOnErrorCallback(t *testing.T) {
	var capturedErr error
	var capturedEvent pulse.PulseEvent

	// Create a separate server for this test since we need to shut it down manually
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create test server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}

	d, err := NewDispatcher(&Config{
		URL:           ns.ClientURL(),
		SubjectPrefix: "pulse",
		StreamName:    "TEST",
		CreateStream:  true,
		StreamConfig: &jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"pulse.>"},
		},
		OnError: func(event pulse.PulseEvent, err error) {
			capturedErr = err
			capturedEvent = event
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	// Close connection to cause publish error
	d.conn.Close()
	ns.Shutdown()
	ns.WaitForShutdown()

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
	_, url := startTestServer(t)

	d, err := NewDispatcher(&Config{
		URL:              url,
		SubjectPrefix:    "test",
		StreamName:       "TEST",
		CreateStream:     true,
		IncludeFullModel: true,
		StreamConfig: &jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	cons, err := d.js.CreateOrUpdateConsumer(context.Background(), "TEST", jetstream.ConsumerConfig{
		Durable:       "test-consumer-full",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "test.hotels.create",
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
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

	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("failed to fetch: %v", err)
	}

	for msg := range msgs.Messages() {
		var received map[string]any
		if err := json.Unmarshal(msg.Data(), &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		// With IncludeFullModel, data should be included
		if received["data"] == nil {
			t.Error("expected data to be included with IncludeFullModel")
		}
		msg.Ack()
	}
}
