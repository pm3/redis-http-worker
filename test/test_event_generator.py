"""
Event generator for testing the worker.
Publishes events to Redis streams.
"""
import asyncio
import json
import os
import random
import string
from datetime import datetime, UTC
from typing import Set

import redis.asyncio as redis

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

NUM_EVENTS = int(os.getenv("NUM_EVENTS", "10000"))

def generate_random_string(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

async def publish_event_to_redis(r: redis.Redis, event_id: str, event_type: str, 
                                  data: str, stream: str = "events:default", 
                                  retries_left: int = 30):
    created_at = datetime.now(UTC).isoformat()
    
    meta = {
        "id": event_id,
        "type": event_type,
        "createdAt": created_at,
    }
    
    await r.xadd(stream, {
        "meta": json.dumps(meta),
        "payload": data
    })


async def event_generator(r: redis.Redis, sent_event_ids: Set[str]):
    print(f"\n📤 Starting event generator ({NUM_EVENTS} events)...")
    
    event_types = ["user_created", "user_updated", "order_placed", "payment_processed"]
    streams = ["events:critical", "events:fast", "events:default", "events:slow"]
    
    for i in range(NUM_EVENTS):
        event_id = str(i)
        event_type = random.choice(event_types)
        data = json.dumps({
            "index": i,
            "random_data": generate_random_string(50),
            "timestamp": datetime.now(UTC).isoformat()
        })
        stream = random.choice(streams)
        
        await publish_event_to_redis(r, event_id, event_type, data, stream)
        sent_event_ids.add(event_id)
        print(f"✅ Sent event {i} of {NUM_EVENTS}", flush=True)


    print(f"✅ Event generator completed - sent {NUM_EVENTS} events", flush=True)

    count = 0
    while count < 60 :
        OK = int(await r.get("http_server:ok"))
        if OK == NUM_EVENTS:
            break
        await asyncio.sleep(1)
        count += 1
        print(f"✅ Check all events sent: {OK} != {NUM_EVENTS}", flush=True)
        
    if OK != NUM_EVENTS:
        print(f"❌ Failed to send all events: {OK} != {NUM_EVENTS}", flush=True)
        exit(1)
    else:
        print(f"✅ All events sent: {OK} == {NUM_EVENTS}", flush=True)
        exit(0)

async def main():
    print("=" * 70, flush=True)
    print("🧪 HTTP Worker Event Generator", flush=True)
    print("=" * 70, flush=True)
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        await r.ping()
        print("✅ Connected to Redis", flush=True)
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}", flush=True)
        return
    
    sent_event_ids: Set[str] = set()
    
    try:
        await event_generator(r, sent_event_ids)       
    finally:
        await r.close()
        print("\n🧹 Cleanup completed", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
