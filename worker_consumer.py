import os
import redis.asyncio as redis
import asyncio
import time
import json
import httpx
import datetime
from metrics import event_age_seconds, event_processing_duration, events_processed, events_failed
from dlq import AbstractDlqManager
from utils import BaseConsumer, print_msg, wait_for_redis

RETRYABLE_NOT_AVAILABLE = { 502, 503, 504 }

class Message:
    __slots__ = ["msg_id", "fields", "meta", "event_id", "payload", "event_type", "err_count_key"]
    def __init__(self, msg_id: str, fields: dict, stream: str):
        self.msg_id = msg_id
        self.fields = fields
        self.meta = json.loads(fields["meta"])
        self.event_id = str(self.meta["id"]) if "id" in self.meta else None
        self.payload = fields.get("payload")
        self.event_type = self.meta.get("type", "unknown")
        self.err_count_key = f"err_count:{stream}:{self.event_id}"


class WorkerConsumer(BaseConsumer):
    def __init__(self, 
        redis: redis.Redis, 
        http: httpx.AsyncClient,
        connect_timeout: float,
        dlq: AbstractDlqManager,
        stream: str, 
        group: str, 
        consumer: str, 
        default_worker_uri: str,
    ):
        super().__init__(redis, http, connect_timeout, dlq)
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.default_worker_uri = default_worker_uri

    async def process_message(self, message: Message):
        now = time.time()

        if "createdAt" in message.meta:
            event_created_at = datetime.datetime.fromisoformat(message.meta["createdAt"])
            age_seconds = (datetime.datetime.now(datetime.UTC) - event_created_at).total_seconds()
            event_age_seconds.labels(stream=self.stream, type=message.event_type).observe(age_seconds)

        # set custom timeout if timeout is in fields
        custom_timeout = None
        if "timeout" in message.fields:
            try:
                custom_timeout = httpx.Timeout(float(message.fields["timeout"]), connect=self.connect_timeout)
            except ValueError:
                custom_timeout = None
        # set url
        url = message.meta.get("url", self.default_worker_uri)
        if "type" in message.meta and "[EVENT_TYPE]" in url:
            url = url.replace("[EVENT_TYPE]", message.event_type)
        # set headers
        headers = {
            "X-Event-Type": message.event_type,
            "X-Event-Queue": message.meta.get("queue", self.stream),
            "Content-Type": "application/octet-stream",
        }
        if message.event_id:
            headers["X-Event-Id"] = message.event_id

        print_msg(f"Processing [event={message.event_id}] type={message.event_type} url={url}")
        try:
            resp = await self.http.post(
                url,
                content=message.payload.encode("utf-8"),
                headers=headers,
                timeout=custom_timeout,
            )
        except Exception as e:
            # retryable
            print_msg(f"[event={message.event_id}] type={message.event_type} error: {e}")
            await self.clean_message(message, False)
            await self.process_error_message(message, f"http_timeout_or_conn_error", 0)
            events_failed.labels(stream=self.stream, type=message.event_type, reason=f"http_timeout_or_conn_error").inc()
            return url

        duration = time.time() - now
        print_msg(f"Processed [event={message.event_id}] type={message.event_type} duration={duration:.3f} status={resp.status_code}")
        event_processing_duration.labels(type=message.event_type).observe(duration)

        # success
        if 200 <= resp.status_code < 300:
            await self.clean_message(message, True)
            await self.dlq.clean_event(message.event_id)
            events_processed.labels(stream=self.stream, type=message.event_type).inc()
            return None

        print_msg(f"Error response [event={message.event_id}]: {resp.status_code}\\n{resp.text.replace('\n', '\\n').replace('\r', '')}")
        events_failed.labels(stream=self.stream, type=message.event_type, reason=f"http_{resp.status_code}").inc()

        # user retryable, retry-after is in seconds
        retry_time = resp.headers.get("X-Retry-After", "")
        if resp.status_code==400 and retry_time.isdigit():            
            await self.clean_message(message, False)
            await self.process_error_message(message, f"http_{resp.status_code}", int(retry_time))
            return None

        # network retryable
        if resp.status_code in RETRYABLE_NOT_AVAILABLE:
            await self.clean_message(message, False)
            await self.process_error_message(message, f"http_{resp.status_code}", 0)
            return url

        # non-retryable
        await self.clean_message(message, True)
        await self.dlq.send_to_dlq(message.event_id, message.event_type, message.payload, f"http_{resp.status_code}")
        print_msg(f"sent to DLQ [event={message.event_id}] type={message.event_type} status={resp.status_code}")
        return None

    async def clean_message(self, message: Message, delete_err_count: bool):
        await self.redis.xack(self.stream, self.group, message.msg_id)
        await self.redis.xdel(self.stream, message.msg_id)
        if delete_err_count:
            await self.redis.delete(message.err_count_key)

    async def process_error_message(self, message: Message, reason: str, retry_time: int):
        err_count = await self.redis.incr(message.err_count_key)
        if err_count >= 5:
            print_msg(f"max retries reached [event={message.event_id}] type={message.event_type} status={reason}")
            await self.redis.delete(message.err_count_key)
            await self.dlq.send_to_dlq(message.event_id, message.event_type, message.payload, reason)
            print_msg(f"sent to DLQ [event={message.event_id}] type={message.event_type} status={reason}")
            return
        if retry_time > 0:
            await self.schedule_retry(message, retry_time)
            print_msg(f"scheduled retry [event={message.event_id}] type={message.event_type} retry_time={retry_time} err_count={err_count}")
            return
        #retry now
        await self.redis.xadd(self.stream, {"meta": message.fields["meta"], "payload": message.payload})
        print_msg(f"retry now [event={message.event_id}] type={message.event_type} status={reason}")
        return

    async def schedule_retry(self, message: Message, delay: int):
        next_attempt_ts = int(time.time()) + delay
        meta = {**message.meta, "nextAttemptAt": next_attempt_ts}
        fields = {
            "meta": json.dumps(meta), 
            "stream": self.stream, 
            "payload": message.payload
        }
        retry_id = await self.redis.xadd("events:retry", fields)
        await self.redis.zadd("events:retry:delays", {retry_id: next_attempt_ts})

    async def consume_stream(self, shutdown_event: asyncio.Event):
        await self.ensure_group()

        consumer = f"{self.consumer}-{self.stream}-{os.getpid()}"
        wait_ready_host = None

        while not shutdown_event.is_set():

            if wait_ready_host:
                if await self.check_ready_host(wait_ready_host):
                    wait_ready_host = None
                else:
                    await asyncio.sleep(1)
                    continue

            try:
                # XREADGROUP returns: [(stream_name, [(id, {field:value})...])]
                res = await self.redis.xreadgroup(
                    self.group,
                    consumer,
                    streams={self.stream: ">"},
                    count=1,       # ALWAYS ONE event from this queue
                    block=2000     # ms
                )

                if not res:
                    continue

                for _, messages in res:
                    for msg_id, fields in messages:
                        # ALWAYS process sequentially per stream:
                        message = Message(msg_id, fields, self.stream)
                        wait_ready_host = await self.process_message(message)

            except redis.ResponseError as e:
                if "NOGROUP" in str(e):
                    try:
                        await self.ensure_group()
                    except Exception as e:
                        print_msg(f"Failed to ensure group for stream {self.stream}: {e}")
                await wait_for_redis(self.redis)

            except Exception as e:
                print_msg(f"Failed to read message from stream {self.stream}: {e}")
                await wait_for_redis(self.redis)
