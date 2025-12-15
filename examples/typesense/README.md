# Typesense Example

This example demonstrates gormpulse with Typesense for real-time search index synchronization.

## Quick Start

```bash
# Start Typesense
docker compose up -d

# Wait for Typesense to be healthy
docker compose ps

# Create the hotels collection schema first
curl "http://localhost:18108/collections" \
  -X POST \
  -H "Content-Type: application/json" \
  -H "X-TYPESENSE-API-KEY: xyz123" \
  -d '{
    "name": "hotels",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "city", "type": "string", "facet": true},
      {"name": "country", "type": "string", "facet": true},
      {"name": "star_rating", "type": "int32", "facet": true},
      {"name": "price_per_night", "type": "float"},
      {"name": "amenities", "type": "string"},
      {"name": "description", "type": "string"}
    ]
  }'

# Run the example
TYPESENSE_HOST=http://localhost:18108 TYPESENSE_API_KEY=xyz123 go run main.go
```

## Verify Data in Typesense

```bash
# Search all documents
curl "http://localhost:18108/collections/hotels/documents/search?q=*&query_by=name" \
  -H "X-TYPESENSE-API-KEY: xyz123"

# Get specific document
curl "http://localhost:18108/collections/hotels/documents/hotel-001" \
  -H "X-TYPESENSE-API-KEY: xyz123"
```

## Ports

| Service | Host Port | Container Port |
|---------|-----------|----------------|
| Typesense API | 18108 | 8108 |

## Cleanup

```bash
docker compose down -v  # -v removes the volume with indexed data
```
