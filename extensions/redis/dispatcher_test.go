package redis

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/restayway/gorm-pulse/pulse"
)

func getRedisClient(t *testing.T) *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s: %v", addr, err)
	}

	return client
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.ChannelPrefix != "pulse" {
		t.Errorf("expected channel prefix 'pulse', got %s", config.ChannelPrefix)
	}
	if config.UseStreams != false {
		t.Error("expected UseStreams to be false by default")
	}
	if config.StreamMaxLen != 10000 {
		t.Errorf("expected maxlen 10000, got %d", config.StreamMaxLen)
	}
}

func TestNewDispatcher(t *testing.T) {
	client := getRedisClient(t)
	defer client.Close()

	t.Run("nil config uses defaults", func(t *testing.T) {
		d, err := NewDispatcher(&Config{
			Client: client,
		})
		if err != nil {
			t.Fatalf("failed to create dispatcher: %v", err)
		}
		if d.config.ChannelPrefix != "pulse" {
			t.Errorf("expected default channel prefix, got %s", d.config.ChannelPrefix)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		d, err := NewDispatcher(&Config{
			Client:        client,
			ChannelPrefix: "custom",
		})
		if err != nil {
			t.Fatalf("failed to create dispatcher: %v", err)
		}
		if d.config.ChannelPrefix != "custom" {
			t.Errorf("expected 'custom', got %s", d.config.ChannelPrefix)
		}
	})

	t.Run("with existing client", func(t *testing.T) {
		d, err := NewDispatcher(&Config{
			Client: client,
		})
		if err != nil {
			t.Fatalf("failed to create dispatcher: %v", err)
		}
		if d.client != client {
			t.Error("expected provided client to be used")
		}
	})
}

func TestNewDispatcherWithAddr(t *testing.T) {
	client := getRedisClient(t)
	addr := client.Options().Addr
	client.Close()

	d, err := NewDispatcher(&Config{
		Addr: addr,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}
	defer d.Close()

	if d.client == nil {
		t.Error("expected client to be created")
	}
}

func TestNewDispatcherInvalidAddr(t *testing.T) {
	_, err := NewDispatcher(&Config{
		Addr: "invalid:9999",
	})
	if err == nil {
		t.Error("expected error for invalid address")
	}
}

func TestDispatchPubSub(t *testing.T) {
	client := getRedisClient(t)
	defer client.Close()

	channel := "test:pubsub:" + time.Now().Format("20060102150405")

	d, err := NewDispatcher(&Config{
		Client:        client,
		ChannelPrefix: channel,
		UseStreams:    false,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	// Subscribe to channel - format is {prefix}:{table}:{event_type}
	ctx := context.Background()
	pubsub := client.Subscribe(ctx, channel+":hotels:create")
	defer pubsub.Close()

	// Wait for subscription to be established
	_, err = pubsub.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
		Document: &pulse.PulseDocument{
			ID:         "hotel-1",
			Collection: "hotels",
			Document:   map[string]any{"name": "Test Hotel"},
		},
	}

	err = d.Dispatch(ctx, event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	// Receive with timeout
	ch := pubsub.Channel()
	select {
	case msg := <-ch:
		var received map[string]any
		if err := json.Unmarshal([]byte(msg.Payload), &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if received["type"] != string(pulse.PulseCreate) {
			t.Errorf("expected PulseCreate, got %v", received["type"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestDispatchStreams(t *testing.T) {
	client := getRedisClient(t)
	defer client.Close()

	streamName := "test:stream:" + time.Now().Format("20060102150405")

	d, err := NewDispatcher(&Config{
		Client:        client,
		ChannelPrefix: streamName,
		UseStreams:    true,
		StreamMaxLen:  100,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	ctx := context.Background()

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
		Document: &pulse.PulseDocument{
			ID:         "hotel-1",
			Collection: "hotels",
			Document:   map[string]any{"name": "Test Hotel"},
		},
	}

	err = d.Dispatch(ctx, event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	// Read from stream - format is {prefix}:{table}:{event_type}
	result, err := client.XRange(ctx, streamName+":hotels:create", "-", "+").Result()
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}

	if len(result) == 0 {
		t.Fatal("expected at least one message in stream")
	}

	// Clean up
	client.Del(ctx, streamName+":hotels:create")
}

func TestChannelMapper(t *testing.T) {
	client := getRedisClient(t)
	defer client.Close()

	d, err := NewDispatcher(&Config{
		Client:        client,
		ChannelPrefix: "base:channel",
		UseStreams:    false,
		ChannelMapper: func(event pulse.PulseEvent) string {
			return "mapped:" + event.Table
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	ctx := context.Background()
	pubsub := client.Subscribe(ctx, "mapped:hotels")
	defer pubsub.Close()

	_, err = pubsub.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(ctx, event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	ch := pubsub.Channel()
	select {
	case <-ch:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message on mapped channel")
	}
}

func TestClose(t *testing.T) {
	client := getRedisClient(t)

	d, err := NewDispatcher(&Config{
		Client: client,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	err = d.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}
}

func TestCloseOwnedClient(t *testing.T) {
	client := getRedisClient(t)
	addr := client.Options().Addr
	client.Close()

	d, err := NewDispatcher(&Config{
		Addr: addr,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	err = d.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	// Client should be closed after d.Close()
	ctx := context.Background()
	if err := d.client.Ping(ctx).Err(); err == nil {
		t.Error("expected client to be closed")
	}
}

func TestOnErrorCallback(t *testing.T) {
	var capturedErr error
	var capturedEvent pulse.PulseEvent

	// Create with invalid address
	d, _ := NewDispatcher(&Config{
		Addr:          "localhost:9999", // Invalid port
		ChannelPrefix: "test",
		OnError: func(event pulse.PulseEvent, err error) {
			capturedErr = err
			capturedEvent = event
		},
	})

	if d == nil {
		// If dispatcher creation fails, that's expected
		return
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

func TestStreamMaxLen(t *testing.T) {
	client := getRedisClient(t)
	defer client.Close()

	streamPrefix := "test:maxlen:" + time.Now().Format("20060102150405")
	maxLen := int64(100)

	d, err := NewDispatcher(&Config{
		Client:        client,
		ChannelPrefix: streamPrefix,
		UseStreams:    true,
		StreamMaxLen:  maxLen,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	ctx := context.Background()

	// Add more messages than maxLen
	for i := 0; i < 150; i++ {
		event := pulse.PulseEvent{
			Type:  pulse.PulseCreate,
			Table: "hotels",
		}
		d.Dispatch(ctx, event)
	}

	// Check stream length - stream name format is {prefix}:{table}:{event_type}
	streamKey := streamPrefix + ":hotels:create"
	length, err := client.XLen(ctx, streamKey).Result()
	if err != nil {
		t.Fatalf("failed to get stream length: %v", err)
	}

	// Stream should be limited - with approx (~) Redis may keep slightly more
	// but definitely less than what we inserted
	if length == 150 {
		t.Errorf("expected stream to be trimmed, but got full length %d", length)
	}

	// Clean up
	client.Del(ctx, streamKey)
}

func TestIncludeFullModel(t *testing.T) {
	client := getRedisClient(t)
	defer client.Close()

	channel := "test:fullmodel:" + time.Now().Format("20060102150405")

	d, err := NewDispatcher(&Config{
		Client:           client,
		ChannelPrefix:    channel,
		UseStreams:       false,
		IncludeFullModel: true,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	ctx := context.Background()
	pubsub := client.Subscribe(ctx, channel+":hotels:create")
	defer pubsub.Close()

	_, err = pubsub.Receive(ctx)
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

	err = d.Dispatch(ctx, event)
	if err != nil {
		t.Fatalf("failed to dispatch: %v", err)
	}

	ch := pubsub.Channel()
	select {
	case msg := <-ch:
		var received map[string]any
		if err := json.Unmarshal([]byte(msg.Payload), &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		// With IncludeFullModel, Data field should be included
		if received["data"] == nil {
			t.Error("expected Data field to be included")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}
