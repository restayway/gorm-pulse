// Package typesense provides a Typesense dispatcher for gormpulse.
// It synchronizes database changes to Typesense collections in real-time.
package typesense

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/restayway/gorm-pulse/pulse"
)

// Config holds the configuration for the Typesense dispatcher.
type Config struct {
	// Host is the Typesense server URL (e.g., "http://localhost:8108").
	Host string

	// APIKey is the Typesense API key for authentication.
	APIKey string

	// Timeout is the HTTP request timeout. Default: 10 seconds.
	Timeout time.Duration

	// RetryCount is the number of retry attempts for failed requests.
	// Default: 3
	RetryCount int

	// RetryDelay is the delay between retry attempts.
	// Default: 100ms
	RetryDelay time.Duration

	// HTTPClient allows using a custom HTTP client.
	// Default: http.DefaultClient with configured timeout
	HTTPClient *http.Client

	// OnError is called when an error occurs.
	// Default: nil (errors are returned)
	OnError func(event pulse.PulseEvent, err error)

	// CollectionMapper allows custom collection name mapping.
	// By default, uses the collection from PulseDocument.
	CollectionMapper func(event pulse.PulseEvent) string

	// DocumentTransformer allows modifying documents before indexing.
	// Default: nil (documents are indexed as-is)
	DocumentTransformer func(event pulse.PulseEvent, doc map[string]any) map[string]any
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Timeout:    10 * time.Second,
		RetryCount: 3,
		RetryDelay: 100 * time.Millisecond,
	}
}

// Dispatcher implements pulse.Dispatcher for Typesense.
// It handles create, update, and delete operations by syncing
// documents to Typesense collections.
type Dispatcher struct {
	config *Config
	client *http.Client
}

// NewDispatcher creates a new Typesense dispatcher.
func NewDispatcher(config *Config) *Dispatcher {
	if config == nil {
		config = DefaultConfig()
	}

	// Set defaults
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.RetryCount == 0 {
		config.RetryCount = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 100 * time.Millisecond
	}

	// Use custom client or create one with timeout
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: config.Timeout,
		}
	}

	return &Dispatcher{
		config: config,
		client: client,
	}
}

// Dispatch implements pulse.Dispatcher.
// It routes events to the appropriate Typesense operation.
func (d *Dispatcher) Dispatch(ctx context.Context, event pulse.PulseEvent) error {
	// Skip events without document data
	if event.Document == nil {
		return nil
	}

	// Get collection name
	collection := event.Document.Collection
	if d.config.CollectionMapper != nil {
		collection = d.config.CollectionMapper(event)
	}

	if collection == "" {
		return fmt.Errorf("typesense: collection name is required")
	}

	var err error
	switch event.Type {
	case pulse.PulseCreate:
		err = d.upsertDocument(ctx, collection, event)
	case pulse.PulseUpdate:
		err = d.upsertDocument(ctx, collection, event)
	case pulse.PulseDelete:
		err = d.deleteDocument(ctx, collection, event)
	default:
		return fmt.Errorf("typesense: unknown event type: %s", event.Type)
	}

	if err != nil && d.config.OnError != nil {
		d.config.OnError(event, err)
	}

	return err
}

// upsertDocument creates or updates a document in Typesense.
func (d *Dispatcher) upsertDocument(ctx context.Context, collection string, event pulse.PulseEvent) error {
	// Typesense requires 'id' field for upsert to work correctly
	// Without it, Typesense auto-generates an ID and creates a new document
	if event.Document.ID == "" {
		return fmt.Errorf("typesense: document ID is required for upsert (create/update)")
	}

	// Copy the document to avoid modifying the original
	doc := make(map[string]any)
	for k, v := range event.Document.Document {
		doc[k] = v
	}

	// Set the id field in the document body
	doc["id"] = event.Document.ID

	if d.config.DocumentTransformer != nil {
		doc = d.config.DocumentTransformer(event, doc)
	}

	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("typesense: failed to marshal document: %w", err)
	}

	// Use upsert action to handle both create and update
	url := fmt.Sprintf("%s/collections/%s/documents?action=upsert", d.config.Host, collection)

	return d.doRequestWithRetry(ctx, http.MethodPost, url, body)
}

// deleteDocument removes a document from Typesense.
func (d *Dispatcher) deleteDocument(ctx context.Context, collection string, event pulse.PulseEvent) error {
	docID := event.Document.ID
	if docID == "" {
		return fmt.Errorf("typesense: document ID is required for delete")
	}

	url := fmt.Sprintf("%s/collections/%s/documents/%s", d.config.Host, collection, docID)

	return d.doRequestWithRetry(ctx, http.MethodDelete, url, nil)
}

// doRequestWithRetry executes an HTTP request with retry logic.
func (d *Dispatcher) doRequestWithRetry(ctx context.Context, method, url string, body []byte) error {
	var lastErr error

	for attempt := 0; attempt <= d.config.RetryCount; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.config.RetryDelay * time.Duration(attempt)):
			}
		}

		err := d.doRequest(ctx, method, url, body)
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry on client errors (4xx) except 429 (rate limit)
		if httpErr, ok := err.(*HTTPError); ok {
			if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 && httpErr.StatusCode != 429 {
				return err
			}
		}
	}

	return lastErr
}

// doRequest executes a single HTTP request.
func (d *Dispatcher) doRequest(ctx context.Context, method, url string, body []byte) error {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return fmt.Errorf("typesense: failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TYPESENSE-API-KEY", d.config.APIKey)

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("typesense: request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body for error messages
	respBody, _ := io.ReadAll(resp.Body)

	// Check for success (2xx)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	// Handle 404 on delete as success (document already deleted)
	if method == http.MethodDelete && resp.StatusCode == 404 {
		return nil
	}

	return &HTTPError{
		StatusCode: resp.StatusCode,
		Body:       string(respBody),
	}
}

// HTTPError represents an HTTP error response from Typesense.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("typesense: HTTP %d: %s", e.StatusCode, e.Body)
}

// Verify Dispatcher implements pulse.Dispatcher
var _ pulse.Dispatcher = (*Dispatcher)(nil)
