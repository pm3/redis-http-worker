import json
import os
import httpx
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import redis.asyncio as redis
from datetime import datetime, UTC

from abc import abstractmethod

class AbstractDlqManager:

    @abstractmethod
    async def send_to_dlq(self, event_id: str | None, event_type: str, payload: str, reason: str) -> None:
        pass

    @abstractmethod
    async def clean_event(self, event_id: str | None) -> None:
        pass


class DlqManagerConsole(AbstractDlqManager):
    async def send_to_dlq(self, event_id: str | None, event_type: str, payload: str, reason: str) -> None:
        data = {
            "id": event_id if event_id else "",
            "type": event_type,
            "reason": reason,
            "payload": payload,
        }
        print(f"DLQ:{json.dumps(data)}", flush=True)

    async def clean_event(self, event_id: str | None) -> None:
        pass

class DlqManagerHttp(AbstractDlqManager):
    def __init__(self, http: httpx.AsyncClient, dlq_uri: str):
        self.http = http
        self.dlq_uri = dlq_uri

    async def send_to_dlq(self, event_id: str | None, event_type: str, payload: str, reason: str) -> None:
        data = {
            "id": event_id if event_id else "",
            "type": event_type,
            "reason": reason,
            "payload": payload,
        }
        try:
            response = await self.http.post(self.dlq_uri, json=data)
            print(f"event {event_id} type={event_type} sent to DLQ with status {response.status_code}", flush=True)
        except Exception as e:
            print(f"error sending event {event_id} type={event_type} to DLQ: {e}", flush=True)

    async def clean_event(self, event_id: str | None) -> None:
        pass

class DlqManagerDb(AbstractDlqManager):
    def __init__(self, connection: str, table: str, redis: redis.Redis):
        self.engine = create_async_engine(connection)
        self.table = table
        self.redis = redis
        self.sql_mark_dlq = text(f"UPDATE {table} SET dlq = 1 WHERE id = :id")
        self.sql_unmark_dlq = text(f"UPDATE {table} SET dlq = 0 WHERE id = :id")
        self.sql_insert_dlq = text(f"INSERT INTO {table} (id, type, payload, dlq) VALUES (:id, :type, :payload, 1)")
        self.sql_clean = text(f"DELETE FROM {table} WHERE id = :id")

    async def send_to_dlq(self, event_id: str | None, event_type: str, payload: str, reason: str) -> None:
        if event_id is None:
            self.send_to_console(event_id, event_type, payload, reason)
            return
        async with self.engine.connect() as conn:
            result = await conn.execute(self.sql_mark_dlq, {"id": event_id})
            if result.rowcount == 0:
                await conn.execute(self.sql_insert_dlq, {"id": event_id, "type": event_type, "payload": payload})
            await conn.commit()

    async def clean_event(self, event_id: str | None) -> None:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(self.sql_clean, {"id": event_id})
                await conn.commit()
        except Exception as e:
            print(f"Error cleaning event {event_id}: {e}")

    async def reprocess_dlq(self, expr_dict: dict, stream: str) -> list[str]:
        reprocess_log = []
        dry_run = expr_dict.get("dry_run", False)   
        where_clause, values = parse_expr(expr_dict)
        query = f"SELECT id, type, payload FROM {self.table} WHERE dlq = 1 and {where_clause}"
        reprocess_log.append(f"Query: {query}")
        reprocess_log.append(f"Values: {values}")
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), values)
            rows = result.fetchall() 
            reprocess_log.append(f"Found {len(rows)} events to reprocess")
            for row in rows:
                reprocess_log.append(f"reprocess event id {row[0]}")
                if not dry_run:
                    meta = {"id": row[0], "type": row[1], "createdAt": datetime.now(UTC).isoformat()}
                    await conn.execute(self.sql_unmark_dlq, {"id": row[0]})
                    await self.redis.xadd(stream, {"meta": json.dumps(meta), "payload": row[2]})
            await conn.commit()
            reprocess_log.append("Committed changes")
        return reprocess_log

def parse_expr(expr_dict: dict) -> tuple[str, dict]:
    query_conditions = []
    values = {}
    pos = 1
    for key, value in expr_dict.items():
        if key == "gt":
            query_conditions.append(f"id > :p{pos}")
            values[f"p{pos}"] = value
            pos += 1
        elif key == "lt":
            query_conditions.append(f"id < :p{pos}")
            values[f"p{pos}"] = value
            pos += 1
        elif key == "gte":
            query_conditions.append(f"id >= :p{pos}")
            values[f"p{pos}"] = value
            pos += 1
        elif key == "lte":
            query_conditions.append(f"id <= :p{pos}")
            values[f"p{pos}"] = value
            pos += 1
        elif key == "eq":
            query_conditions.append(f"id = :p{pos}")
            values[f"p{pos}"] = value
            pos += 1
        elif key == "neq":
            query_conditions.append(f"id != :p{pos}")
            values[f"p{pos}"] = value
            pos += 1
        elif key == "in":
            items = value.split(",")
            query_conditions.append(f"id IN ({','.join([f':p{i}' for i in range(pos, pos + len(items))])})")
            for i, item in enumerate(items):
                values[f"p{pos + i}"] = item
            pos += len(items)
    if len(query_conditions) == 0:
        return "1=1", []
    return " AND ".join(query_conditions), values

