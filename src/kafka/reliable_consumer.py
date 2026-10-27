"""
At-least-once Kafka consumer with retries and a dead-letter queue (aiokafka).

  * Offsets are committed only after a message has been handled (or parked
    in the DLQ), so a crash mid-batch redelivers instead of losing messages.
    Handlers must therefore be idempotent (dedupe by message id).
  * A failing handler is retried with backoff; after `max_attempts` the
    message goes to `<topic>.dlq` with the error, and the partition moves on.
  * Unparseable messages go straight to the DLQ.
  * If Kafka is unavailable the consumer keeps reconnecting with backoff.

The same file is used by decision-engine and insight-engine.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

logger = logging.getLogger(__name__)

Handler = Callable[[Dict[str, Any]], Awaitable[Any]]

try:  # metrics are optional
    from prometheus_client import Counter, Gauge

    PROCESSED = Counter("kafka_consumer_messages_total", "Messages handled", ["topic", "outcome"])
    LAG = Gauge("kafka_consumer_lag", "Messages behind the latest offset", ["topic", "partition"])
except Exception:  # pragma: no cover
    PROCESSED = LAG = None


def connection_kwargs(bootstrap_servers: str | List[str], use_ssl: bool = False,
                      sasl_mechanism: Optional[str] = None, sasl_username: Optional[str] = None,
                      sasl_password: Optional[str] = None) -> Dict[str, Any]:
    kw: Dict[str, Any] = {"bootstrap_servers": bootstrap_servers}
    if sasl_mechanism:
        kw.update(security_protocol="SASL_SSL" if use_ssl else "SASL_PLAINTEXT",
                  sasl_mechanism=sasl_mechanism, sasl_plain_username=sasl_username,
                  sasl_plain_password=sasl_password)
    elif use_ssl:
        kw["security_protocol"] = "SSL"
    if use_ssl:
        kw["ssl_context"] = ssl.create_default_context()
    return kw


def _loads(raw: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    try:
        value = json.loads(raw.decode("utf-8"))
        if isinstance(value, str):  # some producers double-encode
            value = json.loads(value)
        return value if isinstance(value, dict) else None
    except Exception:
        return None


def _count(topic: str, outcome: str) -> None:
    if PROCESSED:
        PROCESSED.labels(topic=topic, outcome=outcome).inc()


class ReliableConsumer:
    def __init__(self, topics: List[str], group_id: str, handlers: Dict[str, Handler],
                 conn: Dict[str, Any], max_attempts: int = 4, dlq_suffix: str = ".dlq",
                 client_id: str = "ceoos-consumer"):
        self.topics = topics
        self.group_id = group_id
        self.handlers = handlers
        self.conn = conn
        self.max_attempts = max_attempts
        self.dlq_suffix = dlq_suffix
        self.client_id = client_id
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.dlq: Optional[AIOKafkaProducer] = None
        self.connected = False
        self._stopping = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"consumer-{self.group_id}")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, 15)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        await self._close()

    async def _close(self) -> None:
        self.connected = False
        for client in (self.consumer, self.dlq):
            if client:
                try:
                    await client.stop()
                except Exception:
                    pass
        self.consumer = self.dlq = None

    async def _run(self) -> None:
        delay = 2
        while not self._stopping.is_set():
            try:
                self.dlq = AIOKafkaProducer(**self.conn, client_id=f"{self.client_id}-dlq",
                                            acks="all", enable_idempotence=True)
                await self.dlq.start()
                self.consumer = AIOKafkaConsumer(
                    *self.topics, **self.conn, group_id=self.group_id, client_id=self.client_id,
                    enable_auto_commit=False, auto_offset_reset="earliest",
                    max_poll_records=200, session_timeout_ms=30000, heartbeat_interval_ms=10000,
                )
                await self.consumer.start()
                self.connected = True
                delay = 2
                logger.info(f"Kafka consumer '{self.group_id}' started: {self.topics}")
                await self._loop()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Kafka consumer '{self.group_id}' error ({e}); reconnecting in {delay}s")
            await self._close()
            try:
                await asyncio.wait_for(self._stopping.wait(), delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)

    async def _loop(self) -> None:
        polls = 0
        while not self._stopping.is_set():
            batches = await self.consumer.getmany(timeout_ms=1000, max_records=200)
            for tp, records in batches.items():
                for record in records:
                    await self._handle(record)
                # Commit this partition only after every record is handled/parked.
                await self.consumer.commit({tp: records[-1].offset + 1})
            polls += 1
            if LAG and polls % 15 == 0:
                await self._report_lag()

    async def _handle(self, record) -> None:
        topic = record.topic
        payload = _loads(record.value)
        if payload is None:
            await self._dead_letter(record, "unparseable message")
            return
        handler = self.handlers.get(topic)
        if handler is None:
            _count(topic, "ignored")
            return
        error = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                await handler(payload)
                _count(topic, "ok")
                return
            except Exception as e:
                error = e
                logger.warning(f"Handler for {topic} failed (attempt {attempt}/{self.max_attempts}): {e}")
                if attempt < self.max_attempts:
                    await asyncio.sleep(min(0.5 * 2 ** (attempt - 1), 8))
        await self._dead_letter(record, f"{type(error).__name__}: {error}")

    async def _dead_letter(self, record, error: str) -> None:
        envelope = {
            "error": error[:1000],
            "source_topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "consumer_group": self.group_id,
            "value": (record.value or b"").decode("utf-8", "replace"),
        }
        # If the DLQ write fails we raise: the offset is not committed and the
        # message is redelivered after reconnect (nothing is silently dropped).
        await self.dlq.send_and_wait(record.topic + self.dlq_suffix,
                                     key=record.key, value=json.dumps(envelope).encode())
        _count(record.topic, "dead_lettered")
        logger.error(f"Message {record.topic}[{record.partition}]@{record.offset} sent to DLQ: {error}")

    async def _report_lag(self) -> None:
        try:
            parts = list(self.consumer.assignment())
            if not parts:
                return
            ends = await self.consumer.end_offsets(parts)
            for tp in parts:
                pos = await self.consumer.position(tp)
                LAG.labels(topic=tp.topic, partition=str(tp.partition)).set(max(ends[tp] - pos, 0))
        except Exception:
            pass


__all__ = ["ReliableConsumer", "connection_kwargs", "TopicPartition"]
