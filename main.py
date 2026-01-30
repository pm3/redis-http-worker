import os
import asyncio
import signal

import httpx
import redis.asyncio as redis
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from metrics import redis_metrics
from dlq import DlqManagerDb, DlqManagerHttp, DlqManagerConsole
from scheduler_loop import SchedulerLoop
from utils import print_msg, wait_for_redis
from worker_consumer import WorkerConsumer

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

async def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

    # wait for redis to be ready
    await wait_for_redis(r)

    # start metrics server
    app = web.Application()

    # health endpoint
    async def health(request: web.Request):
        return web.Response(text="{\"status\": \"ok\"}", headers={"Content-Type": "application/json"})
    app.router.add_get("/health", health)

    # metrics endpoint
    async def metrics(request: web.Request):
        data = generate_latest()
        redis_data = await redis_metrics(r)
        return web.Response(body=data + redis_data, headers={"Content-Type": CONTENT_TYPE_LATEST})
    app.router.add_get("/metrics", metrics)

    # create dlq manager if not already created
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

    timeout = httpx.Timeout(READ_TIMEOUT, connect=CONNECT_TIMEOUT)

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
        for stream in STREAMS:
            consumer = WorkerConsumer(r, http, CONNECT_TIMEOUT, dlq, stream, GROUP, CONSUMER_PREFIX, DEFAULT_WORKER_URI)
            tasks.append(asyncio.create_task(consumer.consume_stream(shutdown_event)))

        # scheduler task
        scheduler = SchedulerLoop(r, http, CONNECT_TIMEOUT, dlq)
        tasks.append(asyncio.create_task(scheduler.loop(shutdown_event)))

        await asyncio.gather(*tasks)

def _handle_sig(*_):
    shutdown_event.set()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)
    asyncio.run(main())