import redis.asyncio as redis
import asyncio
import time
import httpx
import json
from dlq import AbstractDlqManager
from utils import BaseConsumer, print_msg, wait_for_redis

class SchedulerLoop(BaseConsumer):
    def __init__(self, redis: redis.Redis, http: httpx.AsyncClient, connect_timeout: float, dlq: AbstractDlqManager):
        super().__init__(redis, http, connect_timeout, dlq)

    async def loop(self, shutdown_event: asyncio.Event):
        """
        Moves due retry events back from retry stream to their target stream.
        Runs once per second.
        """
        while not shutdown_event.is_set():

            try:
                now = time.time()
                due_ids = await self.redis.zrangebyscore("events:retry:delays", 0, now, start=0, num=200)

                for retry_id in due_ids:
                    msg = await self.redis.xrange("events:retry", min=retry_id, max=retry_id)
                    if not msg:
                        await self.redis.zrem("events:retry:delays", retry_id)
                        continue

                    _, fields = msg[0]
                    meta = json.loads(fields["meta"])
                    stream = fields.get("stream", "events:default")
                    payload = fields.get("payload", "")

                    if "id" in meta:
                        print_msg(f"Moved scheduled event {meta['id']} to {stream}")
                    await self.redis.xadd(stream, {
                        "meta": json.dumps(meta),
                        "payload": payload,
                    })            
                    await self.redis.zrem("events:retry:delays", retry_id)

                await asyncio.sleep(1.0)
            except Exception as e:
                print_msg(f"Scheduler loop failed: {e}")
                await wait_for_redis(self.redis)
