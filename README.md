# HTTP Worker with Redis Streams

A robust Redis Streams-based HTTP worker system with retry logic, metrics, and deliverability testing.

## Features

- **Multiple Priority Streams**: Critical, Fast, Default, Slow
- **Automatic Retries**: Configurable backoff strategy for failed events
- **Dead Letter Queue (DLQ)**: Failed events are sent to DLQ endpoint
- **Prometheus Metrics**: Monitor event processing, failures, and latency
- **Scheduled Retries**: Support for delayed event retry with X-Retry-After header

## Architecture

- **Worker** (`worker.py`): Main event processor that consumes from Redis Streams and sends HTTP requests
- **Metrics** (`metrics.py`): Prometheus metrics for monitoring
- **Test Suite** (`test/`): Deliverability tests with HTTP server and event generators

## Requirements

- Python >= 3.14
- Redis server
- Dependencies managed via `uv` or `pip`

**OR**

- Docker & Docker Compose (recommended for easy setup)

## Quick Start with Docker (Recommended)

The easiest way to run the complete system with Redis, Worker, and Test.

### Using Docker Compose

```bash
# Navigate to test directory
cd test

# Build and start all services
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

This will:
1. Start Redis on `localhost:6379`
2. Start the Worker and expose metrics on `localhost:9000`
3. Start HTTP server on `localhost:8000`
4. Run the event generator test

### Docker Services

- **redis**: Redis server (port 6379)
- **worker**: Event processor (metrics on port 9000)
- **http-server**: HTTP server for receiving events (port 8000)
- **test**: Event generator that publishes test events

### View Metrics

While the system is running:
```bash
curl http://localhost:9000/metrics
```

### Run Individual Services

If you want to run only specific services:

```bash
cd test

# Only Redis and Worker (no test)
docker-compose up redis worker

# Run test manually after starting Redis and Worker
docker-compose up -d redis worker http-server
docker-compose run --rm test

# Scale workers (multiple instances)
docker-compose up -d --scale worker=3
```

### Customize Test Parameters

You can customize the test via environment variables:

```bash
cd test

# Run with more events
docker-compose run --rm -e NUM_EVENTS=1000 test
```

## Manual Installation (Without Docker)

1. Install dependencies:
```bash
uv sync
# or
pip install -e .
```

2. Make sure Redis is running:
```bash
# Default: localhost:6379
redis-server
```

## Running the Worker

Start the worker to process events from Redis Streams:

```bash
python worker.py
```

### Environment Variables

Configure the worker using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server host |
| `REDIS_PORT` | `6379` | Redis server port |
| `GROUP` | `workers` | Consumer group name |
| `CONSUMER_PREFIX` | `c` | Consumer name prefix |
| `STREAMS` | `critical,fast,default,slow` | Comma-separated stream names |
| `DEFAULT_WORKER_URI` | `http://localhost:8000/events/[EVENT_TYPE]` | Target HTTP endpoint |
| `METRICS_PORT` | `9000` | Prometheus metrics port |
| `CONNECT_TIMEOUT` | `2.0` | HTTP connection timeout (seconds) |
| `READ_TIMEOUT` | `60.0` | HTTP read timeout (seconds) |

### Dead Letter Queue (DLQ) Configuration

The DLQ manager supports three modes for handling failed events:

| Variable | Default | Description |
|----------|---------|-------------|
| `DLQ_URI` | - | HTTP endpoint for sending failed events |
| `DLQ_DB_CONNECTION` | - | SQLAlchemy async database connection string |
| `DLQ_DB_TABLE` | `dlq` | Database table name for storing failed events |
| `EVENT_DB_TABLE` | - | Source event table to clean after moving to DLQ |

**Mode Priority:**
1. **Database** (if `DLQ_DB_CONNECTION` is set) - stores events in database and optionally cleans source table
2. **HTTP** (if `DLQ_URI` is set) - sends events to HTTP endpoint
3. **Console** (default) - prints events to stdout with `DLQ:` prefix

**DLQ Database Table Schema:**
```sql
CREATE TABLE dlq (
    id VARCHAR PRIMARY KEY,
    type VARCHAR NOT NULL,
    reason TEXT NOT NULL,
    payload TEXT NOT NULL
);
```

**Event Cleanup:** When `DLQ_DB_CONNECTION` and `EVENT_DB_TABLE` are set, the worker automatically deletes the original event from the source table after moving it to DLQ:
```sql
DELETE FROM {EVENT_DB_TABLE} WHERE event_id = :event_id
```

### Test Configuration

Configure the test behavior using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_EVENTS` | `10000` | Number of events to generate |
| `SERVER_PORT` | `8000` | HTTP server port |
| `SERVER_HOST` | `http-server` | HTTP server host (for Docker) |

Example:
```bash
REDIS_HOST=redis.example.com STREAMS=critical,fast python worker.py
```

## Running the Deliverability Test

The test suite validates that all events are delivered successfully:

1. **Start Redis** (if not already running)

2. **Start the Worker**:
```bash
python worker.py
```

3. **Start the HTTP Server** (in a separate terminal):
```bash
cd test
python test_http_server.py
```

4. **Run the Event Generator** (in a separate terminal):
```bash
cd test
python test_event_generator.py
```

### What the Test Does

1. **HTTP Server**: Receives events on `localhost:8000/events/{event_type}`
2. **Event Generation**: Creates events across different priority streams
3. **Tracking**: Monitors all sent vs. received events via Redis counter
4. **Verification**: Ensures all events are delivered successfully

### Test Output

```
======================================================================
🧪 HTTP Worker Event Generator
======================================================================
✅ Connected to Redis

📤 Starting event generator (10000 events)...
✅ Sent event 0 of 10000
✅ Sent event 1 of 10000
...
✅ Event generator completed - sent 10000 events
✅ Check all events sent: 9950 != 10000
✅ Check all events sent: 9980 != 10000
✅ All events sent: 10000 == 10000

🧹 Cleanup completed
```

## Event Format

Events in Redis Streams have two fields:

### `meta` (JSON)
```json
{
  "id": "unique-event-id",
  "type": "event_type",
  "createdAt": "2026-01-05T12:00:00.000000"
}
```

### `payload` (string)
The actual event data to be sent to the HTTP endpoint.

## Retry Strategy

Failed events are automatically retried:

### Network Errors (502, 503, 504)
Events are immediately re-added to the stream and the worker waits for the target host to become available before processing more events.

### User-Controlled Retry (400 with X-Retry-After)
If the endpoint returns HTTP 400 with a `X-Retry-After` header (in seconds), the event is scheduled for retry after the specified delay.

### Non-Retryable Errors
All other error status codes result in the event being sent to the Dead Letter Queue (DLQ).

### Retry Scheduling
Scheduled retries are stored in `events:retry` stream with delays tracked in `events:retry:delays` sorted set. The scheduler loop checks every second for due retries.

## Metrics

Prometheus metrics are exposed on port 9000 (configurable):

```bash
curl http://localhost:9000/metrics
```

Available metrics:
- `events_processed_total`: Successfully processed events (labels: stream, type)
- `events_failed_total`: Failed events (labels: stream, type, reason)
- `event_age_seconds`: Time from event creation to processing (labels: stream, type)
- `event_processing_duration_seconds`: HTTP call duration (labels: type)

## Development

### Project Structure
```
.
├── worker.py           # Main worker implementation
├── metrics.py          # Prometheus metrics definitions
├── pyproject.toml      # Python dependencies
├── Dockerfile          # Worker Docker image
├── uv.lock             # Locked dependencies
├── README.md           # This file
└── test/
    ├── docker-compose.yml      # Docker Compose orchestration
    ├── Dockerfile              # Test Docker image
    ├── pyproject.toml          # Test dependencies
    ├── test_http_server.py     # HTTP server for testing
    └── test_event_generator.py # Event generator for testing
```

### Testing Strategy

The test suite ensures:
1. All event types are processed correctly
2. Events from all priority streams are handled
3. No events are lost during processing
4. HTTP endpoints receive proper headers and payloads

## Troubleshooting

### Test Fails with "Connection Refused"

Make sure the worker and HTTP server are running before starting the event generator:
```bash
cd test

# Start worker first
docker-compose up -d redis worker http-server

# Wait a few seconds, then run test
docker-compose run --rm test
```

### Worker Can't Connect to Redis

Check if Redis is running:
```bash
cd test
docker-compose ps
docker-compose exec redis redis-cli ping
```

### View Redis Streams

```bash
# Connect to Redis CLI
docker-compose exec redis redis-cli

# Then inside Redis CLI:
XINFO GROUPS events:default
XLEN events:default
XRANGE events:default - + COUNT 10
```

### Clear Redis Data

```bash
cd test

# Stop all services and remove volumes
docker-compose down -v
```

### Check Metrics

```bash
# View Prometheus metrics
curl http://localhost:9000/metrics
```

## Examples

### Manually Send Event to Redis

```bash
# Connect to Redis
docker-compose exec redis redis-cli

# Add event to stream
XADD events:default * meta '{"id":"test-123","type":"test_event","createdAt":"2026-01-05T12:00:00"}' payload '{"message":"Hello World"}'
```

The worker will automatically pick it up and process it.

### Monitor Event Processing

```bash
cd test

# Terminal 1: View worker logs
docker-compose logs -f worker

# Terminal 2: Run test
docker-compose run --rm test

# Terminal 3: Monitor metrics
watch -n 1 'curl -s http://localhost:9000/metrics | grep events_processed'
```

## Performance

The system is designed for high throughput:
- **Sequential Processing**: Each stream is processed sequentially to maintain order
- **Multiple Streams**: Parallel processing across different priority streams
- **Scalability**: Run multiple worker instances with `docker-compose up -d --scale worker=3`
- **Host Health Check**: Worker waits for target host availability on network errors

Typical performance:
- ~100 events/second per worker per stream
- Sub-second latency for immediate streams
- Configurable timeouts and backoff strategies

## License

[Add your license here]
