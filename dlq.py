import json
import os
import httpx
from sqlalchemy import create_async_engine, text

class DlqManager:
    def init(self, http: httpx.AsyncClient):
        DLQ_URI = os.getenv("DLQ_URI")
        DLQ_DB_CONNECTION = os.getenv("DLQ_DB_CONNECTION")
        DLQ_DB_TABLE = os.getenv("DLQ_DB_TABLE", "dlq")
        DLQ_CLEAN_TABLE = os.getenv("EVENT_DB_TABLE")
        self.executor = self.send_to_console
        if DLQ_URI:
            self.http = http
            self.dlq_uri = DLQ_URI
            self.executor = self.send_to_http
        if DLQ_DB_CONNECTION:
            self.engine = create_async_engine(DLQ_DB_CONNECTION)
            self.sql_insert = text(f"INSERT INTO {DLQ_DB_TABLE} (id, type, reason, payload) VALUES (:id, :type, :reason, :payload)")
            self.sql_clean = text(f"DELETE FROM {DLQ_CLEAN_TABLE} WHERE event_id = :event_id")
            self.executor = self.send_to_db
        
    async def send_to_dlq(self, event: dict, payload: str, reason: str):
        await self.executor(event, payload, reason)
        await self.clean_event(event.get("id", None))

    async def send_to_http(self, event: dict, payload: str, reason: str):
        if payload.startswith("{") or payload.startswith("["):
            try:
                payload = json.dumps(json.loads(payload))
            except:
                pass
        data = {
            "id": event.get("id", ""),
            "type": event.get("type", "unknown"),
            "reason": reason,
            "payload": payload,
        }
        response = await self.http.post(self.dlq_uri, json=data)
        print(f"event {event.get('id', '')} type={event.get('type', 'unknown')} sent to DLQ with status {response.status_code}", flush=True)

    async def send_to_db(self, event: dict, payload: str, reason: str):
        async with self.engine.connect() as conn:
            data = {
                "id": event.get("id", ""),
                "type": event.get("type", "unknown"),
                "reason": reason,
                "payload": payload,
            }
            await conn.execute(self.sql_insert, params=data)
            await conn.commit()

    async def send_to_console(self, event: dict, payload: str, reason: str):
        data = {
            "id": event.get("id", ""),
            "type": event.get("type", "unknown"),
            "reason": reason,
            "payload": payload,
        }
        print(f"DLQ:{json.dumps(data)}", flush=True)

    async def clean_event(self, event_id: str | None):
        if self.sql_clean and event_id:
            async with self.engine.connect() as conn:
                await conn.execute(self.sql_clean, params={"event_id": event_id})
                await conn.commit()