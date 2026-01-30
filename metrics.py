from prometheus_client import Counter, Histogram
import redis.asyncio as redis

# ---------- Prometheus metriky ----------

events_processed = Counter(
    "events_processed_total",
    "Number of successfully processed events",
    ["stream", "type"],
)

events_failed = Counter(
    "events_failed_total",
    "Number of failed events",
    ["stream", "type", "reason"],
)

event_age_seconds = Histogram(
    "event_age_seconds",
    "Time spent in stream (event creation -> processing start)",
    ["stream", "type"],
)

event_processing_duration = Histogram(
    "event_processing_duration_seconds",
    "Time to process an event (HTTP call duration)",
    ["type"],
)


async def redis_metrics(r: redis.Redis):
    lines = []
    lines.append("# HELP php_events_total Event counts stored in Redis by PHP")
    lines.append("# TYPE php_events_total counter")
    cursor = 0
    while True:
        cursor, keys = await r.scan(cursor=cursor, match="metric:*", count=100)
        values = await r.mget(keys)
        for key, value in zip(keys, values):
            key = key.replace("metric:", "")
            lines.append(f"php_events_total{{type=\"{key}\"}} {int(value)}")
        if cursor == 0:
            break

    return "\n".join(lines).encode("utf-8")
