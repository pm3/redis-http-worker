"""
Event generator for testing the worker.
Publishes events to Redis streams and MySQL database.
"""
import asyncio
import json
import os
import random
import string
from datetime import datetime, UTC
from typing import Set

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_CONNECTION = os.getenv("DB_CONNECTION", "mysql+aiomysql://zooza:zooza@localhost:3306/zooza")

NUM_EVENTS = int(os.getenv("NUM_EVENTS", "10000"))


def generate_random_string(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


async def create_tables(engine: AsyncEngine):
    """Create events and dlq tables if they don't exist."""
    async with engine.begin() as conn:
        # Drop tables if exist for clean test
        await conn.execute(text("DROP TABLE IF EXISTS events"))
        
        # Create events table
        await conn.execute(text("""
            CREATE TABLE events (
                id varchar(255) PRIMARY KEY,
                type VARCHAR(255) NOT NULL,
                stream VARCHAR(255) NOT NULL,
                dlq BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                payload TEXT
            )
        """))
        
    print("=" * 70, flush=True)        
    print("✅ Created MySQL tables: events", flush=True)
    print("=" * 70, flush=True)


async def insert_event_to_mysql(engine: AsyncEngine, event_id: str, event_type: str, 
                                 stream: str, payload: str):
    """Insert event into MySQL database."""
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO events (id, type, stream, payload) VALUES (:id, :type, :stream, :payload)"),
            {"id": event_id, "type": event_type, "stream": stream, "payload": payload}
        )


async def select_events(engine: AsyncEngine) -> int:
    print("🔍 Checking events database...", flush=True)
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT id FROM events"))
        rows = result.fetchall()
        for row in rows:
            print(f"remaining event: {row[0]}", flush=True)


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
    
    return created_at


async def event_generator(r: redis.Redis, engine: AsyncEngine, sent_event_ids: Set[str]):
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
        
        # Publish to Redis
        created_at = await publish_event_to_redis(r, event_id, event_type, data, stream)
        
        # Insert to MySQL
        await insert_event_to_mysql(engine, event_id, event_type, stream, data)
        
        sent_event_ids.add(event_id)
        print(f"✅ Sent event {i} of {NUM_EVENTS}", flush=True)

    print(f"✅ Event generator completed - sent {NUM_EVENTS} events", flush=True)

    # Wait for HTTP server to process all events
    count = 0
    while count < 60:
        OK = int(await r.get("http_server:ok") or 0)
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

    # Check MySQL - events should be deleted after processing
    print("\n🔍 Checking MySQL database...", flush=True)
    
    # Wait a bit for worker to clean up events
    await asyncio.sleep(5)
    
    # Check events count with retries
    await select_events(engine)
        

async def main():
    print("=" * 70, flush=True)
    print("🧪 HTTP Worker Event Generator", flush=True)
    print("=" * 70, flush=True)
    
    # Connect to Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        await r.ping()
        print("✅ Connected to Redis", flush=True)
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}", flush=True)
        return
    
    # Connect to MySQL
    print(f"🔌 Connecting to MySQL: {DB_CONNECTION}", flush=True)
    
    engine = create_async_engine(DB_CONNECTION, echo=False)
    
    try:
        await create_tables(engine)
        print("✅ Connected to MySQL", flush=True)
    except Exception as e:
        print(f"❌ Failed to connect to MySQL: {e}", flush=True)
        await r.aclose()
        return
    
    sent_event_ids: Set[str] = set()
    
    try:
        await event_generator(r, engine, sent_event_ids)       
    finally:
        await engine.dispose()
        await r.aclose()
        print("\n🧹 Cleanup completed", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
