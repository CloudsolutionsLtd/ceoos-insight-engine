"""
Outbox-backed Kafka producer.

`send()` writes the message to the `outbox` table (durable as soon as it
returns); a background relay publishes pending rows in order with acks=all
and marks them sent. A Kafka outage delays messages instead of dropping
them. Replicas share the work with FOR UPDATE SKIP LOCKED.

Same file in decision-engine and insight-engine.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)

OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(200) NOT NULL,
    msg_key VARCHAR(300),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    sent_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(id) WHERE sent_at IS NULL;
"""

try:
    from prometheus_client import Gauge

    PENDING = Gauge("outbox_pending", "Outbox messages waiting to be published")
    OLDEST = Gauge("outbox_oldest_pending_seconds", "Age of the oldest unpublished outbox message")
except Exception:  # pragma: no cover
    PENDING = OLDEST = None


class OutboxProducer:
    def __init__(self, pool, conn: Dict[str, Any], client_id: str, poll_seconds: float = 1.0, batch: int = 200):
        self.pool = pool
        self.conn = conn
        self.client_id = client_id
        self.poll = poll_seconds
        self.batch = batch
        self.kafka: Optional[AIOKafkaProducer] = None
        self._stopping = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    @property
    def connected(self) -> bool:
        return self.kafka is not None

    # -- API used by the engines ---------------------------------------------
    async def send(self, topic: str, key: Optional[str] = None, value: Any = None, conn=None) -> None:
        """Queue a message. Pass `conn` to enqueue inside your own transaction."""
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                value = {"value": value}
        sql = "INSERT INTO outbox (topic, msg_key, payload) VALUES ($1, $2, $3)"
        args = (topic, key, json.dumps(value, default=str))
        if conn is not None:
            await conn.execute(sql, *args)
        else:
            async with self.pool.acquire() as c:
                await c.execute(sql, *args)

    async def send_batch(self, topic: str, messages) -> None:
        for m in messages:
            await self.send(topic, value=m)

    # -- relay ---------------------------------------------------------------
    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="outbox-relay")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, 10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        await self._disconnect()

    def close(self) -> None:  # backwards compatibility with the old wrapper
        self._stopping.set()

    async def _connect(self) -> bool:
        if self.kafka:
            return True
        producer = AIOKafkaProducer(
            **self.conn, client_id=self.client_id, acks="all", enable_idempotence=True,
            compression_type="gzip", linger_ms=5,
            key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
        )
        try:
            await producer.start()
        except Exception as e:
            try:
                await producer.stop()
            except Exception:
                pass
            logger.warning(f"Outbox relay: Kafka unavailable ({e})")
            return False
        self.kafka = producer
        logger.info("Outbox relay connected to Kafka")
        return True

    async def _disconnect(self) -> None:
        producer, self.kafka = self.kafka, None
        if producer:
            try:
                await producer.stop()
            except Exception:
                pass

    async def _run(self) -> None:
        delay = self.poll
        loops = 0
        while not self._stopping.is_set():
            sent = 0
            try:
                if await self._connect():
                    sent = await self._publish_batch()
                    delay = self.poll
                else:
                    delay = min(delay * 2, 30)
                loops += 1
                if loops % 30 == 0:
                    await self._housekeeping()
            except Exception as e:
                logger.error(f"Outbox relay error: {e}")
                delay = min(delay * 2, 30)
            if sent >= self.batch:
                continue
            try:
                await asyncio.wait_for(self._stopping.wait(), delay)
            except asyncio.TimeoutError:
                pass

    async def _publish_batch(self) -> int:
        sent = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """SELECT id, topic, msg_key, payload FROM outbox WHERE sent_at IS NULL
                       ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED""", self.batch)
                for row in rows:
                    payload = row["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    try:
                        await self.kafka.send_and_wait(row["topic"], key=row["msg_key"], value=payload)
                    except Exception as e:
                        await conn.execute(
                            "UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1",
                            row["id"], str(e)[:500])
                        await self._disconnect()
                        break
                    await conn.execute("UPDATE outbox SET sent_at = NOW() WHERE id = $1", row["id"])
                    sent += 1
        return sent

    async def _housekeeping(self) -> None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) AS n, EXTRACT(EPOCH FROM NOW() - MIN(created_at)) AS age FROM outbox WHERE sent_at IS NULL")
            if PENDING:
                PENDING.set(row["n"] or 0)
                OLDEST.set(float(row["age"] or 0))
            await conn.execute("DELETE FROM outbox WHERE sent_at < NOW() - INTERVAL '3 days'")
