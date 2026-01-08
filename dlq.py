import json
import os
import httpx
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

class DlqManager:
    def __init__(self, http: httpx.AsyncClient):
        DLQ_URI = os.getenv("DLQ_URI")
        DLQ_DB_CONNECTION = os.getenv("DLQ_DB_CONNECTION")
        DLQ_DB_TABLE = os.getenv("DLQ_DB_TABLE")
        self.executor = self.send_to_console
        self.engine = None
        if DLQ_URI:
            self.http = http
            self.dlq_uri = DLQ_URI
            self.executor = self.send_to_http
        if DLQ_DB_CONNECTION and DLQ_DB_TABLE:
            self.engine = create_async_engine(DLQ_DB_CONNECTION)
            self.sql_mark_dlq = text(f"UPDATE {DLQ_DB_TABLE} SET dlq = 1 WHERE id = :id")
            self.sql_insert_dlq = text(f"INSERT INTO {DLQ_DB_TABLE} (id, type, payload, dlq) VALUES (:id, :type, :payload, 1)")
            self.sql_clean = text(f"DELETE FROM {DLQ_DB_TABLE} WHERE id = :id")
            self.executor = self.send_to_db
        
    async def send_to_dlq(self, event_id: str | None, event_type: str, payload: str, reason: str):
        await self.executor(event_id, event_type, payload, reason)

    async def send_to_http(self, event_id: str | None, event_type: str, payload: str, reason: str):
        if payload.startswith("{") or payload.startswith("["):
            try:
                payload = json.dumps(json.loads(payload))
            except:
                pass
        data = {
            "id": event_id if event_id else "",
            "type": event_type,
            "reason": reason,
            "payload": payload,
        }
        response = await self.http.post(self.dlq_uri, json=data)
        print(f"event {event_id} type={event_type} sent to DLQ with status {response.status_code}", flush=True)

    async def send_to_db(self, event_id: str | None, event_type: str, payload: str, reason: str):
        if event_id is None:
            self.send_to_console(event_id, event_type, payload, reason)
            return
        async with self.engine.connect() as conn:
            result = await conn.execute(self.sql_mark_dlq, {"id": event_id})
            if result.rowcount == 0:
                await conn.execute(self.sql_insert_dlq, {"id": event_id, "type": event_type, "payload": payload})
            await conn.commit()
                
    async def send_to_console(self, event_id: str | None, event_type: str, payload: str, reason: str):
        data = {
            "id": event_id if event_id else "",
            "type": event_type,
            "reason": reason,
            "payload": payload,
        }
        print(f"DLQ:{json.dumps(data)}", flush=True)

    async def clean_event(self, event_id: str | None):
        if self.engine is not None and event_id is not None:
            try:
                async with self.engine.connect() as conn:
                    result = await conn.execute(self.sql_clean, {"id": event_id})
                    if result.rowcount == 0:
                        print(f"event [event={event_id}] not found in database")
                    else:
                        print(f"event [event={event_id}] cleaned in database")
                    await conn.commit()
            except Exception as e:
                print(f"Error cleaning event {event_id}: {e}")