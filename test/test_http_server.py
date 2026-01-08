"""
Standalone HTTP server for testing the worker.
Receives events and tracks them for verification.
"""
import asyncio
import os

from aiohttp import web
import redis.asyncio as redis
# Configuration
SERVER_PORT = int(os.getenv("SERVER_PORT", "8000"))

# Tracking

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


count = { "ok": 0 }
count_lock = asyncio.Lock()

async def http_handler(request: web.Request) -> web.Response:
    """
    HTTP endpoint handler for /events/{event_type}
    Tracks all received events to verify deliverability
    Simulates intermittent failures to test retry logic
    """
    event_type = request.match_info.get('event_type', 'unknown')
    event_id = request.headers.get('X-Event-Id', 'no-id')
    
    # Read payload
    payload = await request.text()

    async with count_lock:
        akt = count["ok"] + 1
        count["ok"] = akt
        
    if akt == 276:
        print(f"✅ Simulated deployment: {count['ok']} = {event_id}", flush=True)
        exit(0)
    if count["ok"] == 123:
        print(f"✅ Simulated retry-after: 20 = {event_id}", flush=True)
        return web.Response(status=400, text="ERROR: retry-after: 20", headers={"X-Retry-After": "20"})

    print(f"✓ Received: id={event_id}, type={event_type}, payload_len={len(payload)}")

    #await asyncio.sleep(0.05)
    #zapis do redis counteru ok
    await redis.incr("http_server:ok")  
    return web.Response(status=200, text="OK")

async def main():
    """Start HTTP server"""
    app = web.Application()
    app.router.add_post('/events/{event_type}', http_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', SERVER_PORT)
    await site.start()
    
    print(f"🚀 HTTP Server started on http://0.0.0.0:{SERVER_PORT}")
    
    # Keep running forever
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())

