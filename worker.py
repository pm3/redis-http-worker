import datetime
import os
import json
import time
import asyncio
import signal

import httpx
import redis.asyncio as redis
from prometheus_client import start_http_server

from metrics import event_age_seconds, event_processing_duration, events_processed, events_failed
from dlq import DlqManager

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

GROUP = os.getenv("GROUP", "workers")
CONSUMER_PREFIX = os.getenv("CONSUMER_PREFIX", "c")

STREAMS_STRING = os.getenv("STREAMS", "critical,fast,default,slow")
STREAMS = [f"events:{s}" for s in STREAMS_STRING.split(",")]

DEFAULT_WORKER_URI = os.getenv("DEFAULT_WORKER_URI", "http://localhost:8000/events/[EVENT_TYPE]")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9000"))

# timeouts
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "2.0"))
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "60.0"))

RETRYABLE_NOT_AVAILABLE = { 502, 503, 504 }

shutdown_event = asyncio.Event()

def now_ts() -> int:
    return int(time.time())

async def ensure_group(r: redis.Redis, stream: str):
    try:
        await r.xgroup_create(stream, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

async def schedule_retry(r: redis.Redis, stream: str, meta: dict, payload: str, delay: int):

    next_attempt_ts = now_ts() + delay
    meta["nextAttemptAt"] = next_attempt_ts

    retry_id = await r.xadd("events:retry", {
        "meta": json.dumps(meta),
        "stream": stream,
        "payload": payload,
    })

    await r.zadd("events:retry:delays", {retry_id: next_attempt_ts})
    print(f"Scheduled retry for event {meta['id']} in {delay} seconds", flush=True)

async def check_ready_host(http: httpx.AsyncClient, url: str) -> bool:
    try:
        resp = await http.options(url, timeout=1, follow_redirects=False)
        print(f"CHECK_READY_HOST: {url} {resp.status_code}", flush=True)
        return resp.status_code in { 200, 204, 404, 405, 301, 302, 303, 307, 308 }
    except Exception as e:
        print(f"CHECK_READY_HOST: {url} {e}", flush=True)
        return False

async def process_message(
    r: redis.Redis,
    http: httpx.AsyncClient,
    dlq: DlqManager,
    stream: str,
    msg_id: str,
    fields: dict
):
    # fields are strings if decode_responses=True
    now = time.time()
    meta = json.loads(fields["meta"])
    payload = fields.get("payload", "")
    event_type = meta.get("type", "unknown")

    if "createdAt" in meta:
        event_created_at = datetime.datetime.fromisoformat(meta["createdAt"])
        age_seconds = (datetime.datetime.now(datetime.UTC) - event_created_at).total_seconds()
        event_age_seconds.labels(stream=stream, type=event_type).observe(age_seconds)

    url = meta.get("url", DEFAULT_WORKER_URI)
    if "type" in meta and "[EVENT_TYPE]" in url:
        url = url.replace("[EVENT_TYPE]", event_type)

    headers = {
        "X-Event-Type": event_type,
        "X-Event-Queue": meta.get("queue", stream),
        "Content-Type": "application/octet-stream",
    }
    if "id" in meta:
        headers["X-Event-Id"] = meta["id"]

    try:
        resp = await http.post(
            url,
            content=payload.encode("utf-8"),  # if payload is text
            headers=headers,
        )

        duration = time.time() - now
        event_processing_duration.labels(type=event_type).observe(duration)

        # success
        if 200 <= resp.status_code < 300:
            await r.xack(stream, GROUP, msg_id)
            events_processed.labels(stream=stream, type=event_type).inc()
            return None

        events_failed.labels(stream=stream, type=event_type, reason=f"http_{resp.status_code}").inc()

        # user retryable, retry-after is in seconds
        retry_time = resp.headers.get("Retry-After", "")
        if resp.status_code==400 and retry_time.isdigit():
            await r.xack(stream, GROUP, msg_id)
            await schedule_retry(r, stream, meta, payload, int(retry_time))
            return None  

        # network retryable
        if resp.status_code in RETRYABLE_NOT_AVAILABLE:
            await r.xack(stream, GROUP, msg_id)
            await r.xadd(stream, {"meta": json.dumps(meta), "payload": payload})
            return url

        # non-retryable
        await r.xack(stream, GROUP, msg_id)
        await dlq.send_to_dlq(meta, payload, f"http_{resp.status_code}")
        return None

    except Exception as e:
        # retryable
        events_failed.labels(stream=stream, type=event_type, reason=f"http_timeout_or_conn_error").inc()
        await r.xack(stream, GROUP, msg_id)
        await r.xadd(stream, {"meta": json.dumps(meta), "payload": payload})
        return url

async def consume_stream(stream: str, r: redis.Redis, http: httpx.AsyncClient, dlq: DlqManager):
    await ensure_group(r, stream)

    consumer = f"{CONSUMER_PREFIX}-{stream}-{os.getpid()}"
    wait_ready_host = None

    while not shutdown_event.is_set():

        if wait_ready_host:
            if await check_ready_host(http, wait_ready_host):
                wait_ready_host = None
            else:
                await asyncio.sleep(1)
                continue

        # XREADGROUP returns: [(stream_name, [(id, {field:value})...])]
        res = await r.xreadgroup(
            GROUP,
            consumer,
            streams={stream: ">"},
            count=1,       # ALWAYS ONE event from this queue
            block=5000     # ms
        )

        if not res:
            continue

        for _, messages in res:
            for msg_id, fields in messages:
                # ALWAYS process sequentially per stream:
                wait_ready_host = await process_message(r, http, dlq, stream, msg_id, fields)


async def scheduler_loop(r: redis.Redis):
    """
    Moves due retry events back from retry stream to their target stream.
    Runs once per second.
    """
    while not shutdown_event.is_set():
        now = now_ts()
        due_ids = await r.zrangebyscore("events:retry:delays", 0, now, start=0, num=200)

        for retry_id in due_ids:
            msg = await r.xrange("events:retry", min=retry_id, max=retry_id)
            if not msg:
                await r.zrem("events:retry:delays", retry_id)
                continue

            _, fields = msg[0]
            meta = json.loads(fields["meta"])
            stream = fields.get("stream", "events:default")
            payload = fields.get("payload", "")

            print(f"Moved retry for event {meta['id']} to {stream}", flush=True)
            await r.xadd(stream, {
                "meta": json.dumps(meta),
                "payload": payload
            })            
            await r.zrem("events:retry:delays", retry_id)

        await asyncio.sleep(1.0)


async def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Ensure groups exist upfront
    for s in STREAMS:
        await ensure_group(r, s)

    timeout = httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT, write=READ_TIMEOUT, pool=READ_TIMEOUT)

    async with httpx.AsyncClient(timeout=timeout) as http:
        dlq = DlqManager(http)
        tasks = []

        # one consumer task per stream
        for s in STREAMS:
            tasks.append(asyncio.create_task(consume_stream(s, r, http, dlq)))

        # scheduler task
        tasks.append(asyncio.create_task(scheduler_loop(r)))

        await asyncio.gather(*tasks)

def _handle_sig(*_):
    shutdown_event.set()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)
    start_http_server(METRICS_PORT)
    asyncio.run(main())