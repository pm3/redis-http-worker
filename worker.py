import datetime
import os
import json
import time
import asyncio
import signal

import httpx
import redis.asyncio as redis
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from metrics import event_age_seconds, event_processing_duration, events_processed, events_failed
from dlq import DlqManagerDb, DlqManagerHttp, DlqManagerConsole, AbstractDlqManager

REDIS_HOST_PORT = os.getenv("REDIS_HOST_PORT", "redis:6379")
REDIS_HOST, REDIS_PORT = REDIS_HOST_PORT.split(":")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

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

def print_msg(msg: str):
    print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {msg}", flush=True)

def now_ts() -> int:
    return int(time.time())

async def ensure_group(r: redis.Redis, stream: str):
    try:
        await r.xgroup_create(stream, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

async def wait_for_redis(r: redis.Redis):
    time_sleep = 1
    while True:
        try:
            await r.ping()
            break
        except Exception as e:
            print(f"Failed to connect to Redis: {e}", flush=True)
            await asyncio.sleep(time_sleep)
            if time_sleep < 10:
                time_sleep+=1

async def schedule_retry(r: redis.Redis, stream: str, meta: dict, payload: str, delay: int):

    next_attempt_ts = now_ts() + delay
    meta["nextAttemptAt"] = next_attempt_ts

    retry_id = await r.xadd("events:retry", {
        "meta": json.dumps(meta),
        "stream": stream,
        "payload": payload,
    })

    await r.zadd("events:retry:delays", {retry_id: next_attempt_ts})


async def check_ready_host(http: httpx.AsyncClient, url: str) -> bool:
    try:
        resp = await http.options(url, timeout=1, follow_redirects=False)
        print_msg(f"Check ready host: {url} status={resp.status_code}")
        return resp.status_code in { 200, 204, 404, 405, 301, 302, 303, 307, 308 }
    except Exception as e:
        print_msg(f"Check ready host: {url} error={e}")
        return False

async def process_message(
    r: redis.Redis,
    http: httpx.AsyncClient,
    dlq: AbstractDlqManager,
    stream: str,
    msg_id: str,
    fields: dict
):
    # fields are strings if decode_responses=True
    now = time.time()
    meta = json.loads(fields["meta"])
    event_id = str(meta["id"]) if "id" in meta else None
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
    if event_id:
        headers["X-Event-Id"] = event_id

    print_msg(f"Processing [event={event_id}] type={event_type} url={url}")
    try:
        resp = await http.post(
            url,
            content=payload.encode("utf-8"),  # if payload is text
            headers=headers,
        )
    except Exception as e:
        # retryable
        print_msg(f"[event={event_id}] type={event_type} error: {e}")
        await r.xack(stream, GROUP, msg_id)
        await r.xadd(stream, {"meta": fields["meta"], "payload": fields["payload"]})
        events_failed.labels(stream=stream, type=event_type, reason=f"http_timeout_or_conn_error").inc()
        return url

    duration = time.time() - now
    print_msg(f"Processed [event={event_id}] type={event_type} duration={duration:.3f} status={resp.status_code}")

    # success
    if 200 <= resp.status_code < 300:
        await r.xack(stream, GROUP, msg_id)
        event_processing_duration.labels(type=event_type).observe(duration)
        events_processed.labels(stream=stream, type=event_type).inc()
        await dlq.clean_event(event_id)
        return None

    print_msg(f"Error response [event={event_id}]: {resp.status_code}\\n{resp.text.replace('\n', '\\n').replace('\r', '')}")
    events_failed.labels(stream=stream, type=event_type, reason=f"http_{resp.status_code}").inc()

    # user retryable, retry-after is in seconds
    retry_time = resp.headers.get("X-Retry-After", "")
    if resp.status_code==400 and retry_time.isdigit():
        await r.xack(stream, GROUP, msg_id)
        await schedule_retry(r, stream, meta, payload, int(retry_time))
        print_msg(f"scheduled retry [event={event_id}] type={event_type} retry_time={retry_time}")
        return None  

    # network retryable
    if resp.status_code in RETRYABLE_NOT_AVAILABLE:
        await r.xack(stream, GROUP, msg_id)
        await r.xadd(stream, {"meta": json.dumps(meta), "payload": payload})
        return url

    # non-retryable
    await r.xack(stream, GROUP, msg_id)
    await dlq.send_to_dlq(event_id, event_type, payload, f"http_{resp.status_code}")
    print_msg(f"sent to DLQ [event={event_id}] type={event_type} status={resp.status_code}")
    return None

async def consume_stream(stream: str, r: redis.Redis, http: httpx.AsyncClient, dlq: AbstractDlqManager):
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

        try:
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

        except Exception as e:
            print_msg(f"Failed to read message from stream {stream}: {e}")
            await wait_for_redis(r)

async def scheduler_loop(r: redis.Redis):
    """
    Moves due retry events back from retry stream to their target stream.
    Runs once per second.
    """
    while not shutdown_event.is_set():

        try:
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
        except Exception as e:
            print(f"Scheduler loop failed: {e}", flush=True)
            await wait_for_redis(r)

async def xtrim_schedule_loop(r: redis.Redis, streams: list[str]):
    while not shutdown_event.is_set():
        remove_id = (now_ts() - 8*3600) * 1000
        for s in streams:
            try:
                await r.xtrim(s, minid=f"{remove_id}-0", approximate=True)
            except Exception as e:
                print(f"XTRIM failed for stream {s}: {e}", flush=True)
        await asyncio.sleep(5*60.0)

async def health(request: web.Request):
    return web.Response(text="{\"status\": \"ok\"}", headers={"Content-Type": "application/json"})

async def metrics(request: web.Request):
    data = generate_latest()
    return web.Response(body=data, headers={"Content-Type": CONTENT_TYPE_LATEST})

async def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

    # wait for redis to be ready
    await wait_for_redis(r)
    # ensure groups exist upfront
    for s in STREAMS:
        await ensure_group(r, s)

    # start metrics server
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/metrics", metrics)

    dlq = None
    DLQ_DB_CONNECTION = os.getenv("DLQ_DB_CONNECTION")
    DLQ_DB_TABLE = os.getenv("DLQ_DB_TABLE")
    if DLQ_DB_CONNECTION and DLQ_DB_TABLE:
        dlq = DlqManagerDb(DLQ_DB_CONNECTION, DLQ_DB_TABLE, r)

        # add reprocess-dlq endpoint - only for db dlq
        async def reprocess_dlq(request: web.Request):
            data = dict(request.query)
            reprocess_log = await dlq.reprocess_dlq(data, "events:default")   
            return web.Response(text="\n".join(reprocess_log), headers={"Content-Type": "text/plain"})
        app.router.add_get("/reprocess-dlq", reprocess_dlq)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", METRICS_PORT)
    await site.start()
    print_msg(f"Server running on :{METRICS_PORT}")

    timeout = httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT, write=READ_TIMEOUT, pool=READ_TIMEOUT)

    async with httpx.AsyncClient(timeout=timeout) as http:

        # create dlq manager if not already created
        if dlq is None:
            DLQ_URI = os.getenv("DLQ_URI")
            if DLQ_URI:
                dlq = DlqManagerHttp(http, DLQ_URI)
            else:
                dlq = DlqManagerConsole()

        tasks = []

        # one consumer task per stream
        for s in STREAMS:
            tasks.append(asyncio.create_task(consume_stream(s, r, http, dlq)))

        # scheduler task
        tasks.append(asyncio.create_task(scheduler_loop(r)))

        # xtrim schedule task
        tasks.append(asyncio.create_task(xtrim_schedule_loop(r, STREAMS)))

        await asyncio.gather(*tasks)

def _handle_sig(*_):
    shutdown_event.set()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)
    asyncio.run(main())