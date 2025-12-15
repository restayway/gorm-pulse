package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/restayway/gorm-pulse/pulse"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.TopicPrefix != "pulse" {
		t.Errorf("expected topic prefix 'pulse', got %s", config.TopicPrefix)
	}
}

func TestNewDispatcherWithMockProducer(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	if d.config.TopicPrefix != "test" {
		t.Errorf("expected 'test', got %s", d.config.TopicPrefix)
	}
}

func TestDispatchCreate(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndSucceed()
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
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
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDispatchUpdate(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndSucceed()
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	event := pulse.PulseEvent{
		Type:   pulse.PulseUpdate,
		Table:  "hotels",
		Before: map[string]any{"name": "Old"},
		After:  map[string]any{"name": "New"},
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDispatchDelete(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndSucceed()
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
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
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDispatchError(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndFail(errors.New("send failed"))
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(context.Background(), event)
	if err == nil {
		t.Error("expected error on send failure")
	}
}

func TestTopicMapper(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		if msg.Topic != "mapped-hotels" {
			t.Errorf("expected topic 'mapped-hotels', got %s", msg.Topic)
		}
		return nil
	})
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
		TopicMapper: func(event pulse.PulseEvent) string {
			return "mapped-" + event.Table
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestKeyMapper(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		key, _ := msg.Key.Encode()
		if string(key) != "hotel-123" {
			t.Errorf("expected key 'hotel-123', got %s", string(key))
		}
		return nil
	})
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
		KeyMapper: func(event pulse.PulseEvent) string {
			if event.Document != nil {
				return event.Document.ID
			}
			return ""
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
		Document: &pulse.PulseDocument{
			ID: "hotel-123",
		},
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMessageEncoding(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		value, _ := msg.Value.Encode()
		var event map[string]any
		if err := json.Unmarshal(value, &event); err != nil {
			t.Errorf("failed to unmarshal event: %v", err)
		}
		if event["type"] != string(pulse.PulseCreate) {
			t.Errorf("expected PulseCreate type, got %v", event["type"])
		}
		return nil
	})
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClose(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	err = d.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}
}

func TestOnErrorCallback(t *testing.T) {
	var capturedErr error
	var capturedEvent pulse.PulseEvent

	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndFail(errors.New("send failed"))
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
		OnError: func(event pulse.PulseEvent, err error) {
			capturedErr = err
			capturedEvent = event
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
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

func TestOnSuccessCallback(t *testing.T) {
	var successCalled bool

	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndSucceed()
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
		OnSuccess: func(event pulse.PulseEvent, partition int32, offset int64) {
			successCalled = true
		},
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !successCalled {
		t.Error("expected OnSuccess callback to be called")
	}
}

func TestDefaultTopicFormat(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		// Default format is {prefix}.{table}.{event_type}
		expectedTopic := "test.hotels.create"
		if msg.Topic != expectedTopic {
			t.Errorf("expected topic '%s', got %s", expectedTopic, msg.Topic)
		}
		return nil
	})
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix: "test",
		Producer:    mockProducer,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIncludeFullModel(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		value, _ := msg.Value.Encode()
		var event map[string]any
		if err := json.Unmarshal(value, &event); err != nil {
			t.Errorf("failed to unmarshal event: %v", err)
		}
		// With IncludeFullModel, Data should be present
		if event["data"] == nil {
			t.Error("expected Data field to be included")
		}
		return nil
	})
	defer mockProducer.Close()

	d, err := NewDispatcher(&Config{
		TopicPrefix:      "test",
		Producer:         mockProducer,
		IncludeFullModel: true,
	})
	if err != nil {
		t.Fatalf("failed to create dispatcher: %v", err)
	}

	event := pulse.PulseEvent{
		Type:  pulse.PulseCreate,
		Table: "hotels",
		After: map[string]any{
			"id":    "hotel-1",
			"name":  "Test Hotel",
			"stars": 5,
		},
	}

	err = d.Dispatch(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
