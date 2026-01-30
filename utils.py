import datetime
import redis.asyncio as redis
import httpx
import asyncio
import time
import json
from dlq import AbstractDlqManager

def print_msg(msg: str):
    print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {msg}", flush=True)

async def wait_for_redis(redis: redis.Redis):
    time_sleep = 1
    while True:
        try:
            await redis.ping()
            break
        except Exception as e:
            print_msg(f"Failed to connect to Redis: {e}")
            await asyncio.sleep(time_sleep)
            if time_sleep < 10:
                time_sleep+=1

class BaseConsumer:
    def __init__(self, redis: redis.Redis, http: httpx.AsyncClient, connect_timeout: float, dlq: AbstractDlqManager):
        self.redis = redis
        self.http = http
        self.connect_timeout = connect_timeout
        self.dlq = dlq

    async def ensure_group(self):
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def check_ready_host(self, url: str) -> bool:
        try:
            resp = await self.http.options(url, timeout=1, follow_redirects=False)
            print_msg(f"Check ready host: {url} status={resp.status_code}")
            return resp.status_code in { 200, 204, 404, 405, 301, 302, 303, 307, 308 }
        except Exception as e:
            print_msg(f"Check ready host: {url} error={e}")
            return False
