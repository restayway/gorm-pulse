# NATS Example

This example demonstrates gormpulse with NATS Core pub/sub messaging.

## Quick Start

```bash
# Start NATS server
docker compose up -d

# Run the example
NATS_URL=nats://localhost:14222 go run main.go

# In another terminal, subscribe to events
nats sub 'gormpulse.>' -s nats://localhost:14222
```

## Ports

| Service | Host Port | Container Port |
|---------|-----------|----------------|
| NATS Client | 14222 | 4222 |
| NATS Monitoring | 18222 | 8222 |

## Cleanup

```bash
docker compose down
```
