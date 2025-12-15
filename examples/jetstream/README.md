# JetStream Example

This example demonstrates gormpulse with NATS JetStream for persistent, reliable messaging.

## Quick Start

```bash
# Start NATS with JetStream enabled
docker compose up -d

# Run the example
NATS_URL=nats://localhost:14223 go run main.go
```

## Features Demonstrated

- Automatic stream creation
- Publish acknowledgments with sequence numbers
- Stream statistics
- Custom stream configuration (retention, max age, storage)

## Ports

| Service | Host Port | Container Port |
|---------|-----------|----------------|
| NATS Client | 14223 | 4222 |
| NATS Monitoring | 18223 | 8222 |

## Consumer Examples

```bash
# Create a durable consumer
nats consumer add GORMPULSE_EVENTS my-consumer \
  --filter 'events.>' \
  --ack explicit \
  --deliver all \
  -s nats://localhost:14223

# Consume messages
nats consumer next GORMPULSE_EVENTS my-consumer --count 10 -s nats://localhost:14223

# Or subscribe ephemerally
nats sub 'events.>' -s nats://localhost:14223
```

## Cleanup

```bash
docker compose down -v  # -v removes the volume with persisted data
```
