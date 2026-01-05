from prometheus_client import Counter, Histogram

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
