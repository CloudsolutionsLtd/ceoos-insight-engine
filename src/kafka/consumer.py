"""
Kafka consumer for the insight engine.

Topics:
  insight-requests, daily-summary-trigger   {account_id, date}  -> generate summary
  transactions (KAFKA_TRANSACTIONS_TOPIC)   transaction         -> local copy
  fraud-alerts (KAFKA_FRAUD_ALERTS_TOPIC)   alert               -> local copy

The summary generator reads `transactions` and `fraud_alerts` from the insight
database, which nothing populated before this consumer stored them.
"""
import asyncio
import json
import logging
from datetime import date, datetime
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaConsumer

from src.config import settings

logger = logging.getLogger(__name__)

TRIGGER_TOPICS = ("insight-requests", "daily-summary-trigger")


def _loads(raw: bytes) -> Optional[Dict[str, Any]]:
    try:
        value = json.loads(raw.decode("utf-8"))
        if isinstance(value, str):
            value = json.loads(value)
        return value if isinstance(value, dict) else None
    except Exception:
        return None


def _ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.utcnow()


class InsightConsumer:
    def __init__(self, summary_generator: Any, db_pool: Any = None):
        self.summary_generator = summary_generator
        self.db = db_pool
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False

    async def start(self) -> None:
        """Connect (retrying while Kafka is unavailable) and consume."""
        self.running = True
        topics = [*TRIGGER_TOPICS, settings.kafka_transactions_topic, settings.kafka_fraud_alerts_topic]
        while self.running:
            try:
                self.consumer = AIOKafkaConsumer(
                    *topics,
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    group_id=settings.kafka_group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    security_protocol="SSL" if settings.kafka_ssl else "PLAINTEXT",
                )
                await self.consumer.start()
                logger.info(f"Insight Kafka consumer started: {topics}")
                await self._consume_loop()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Insight consumer unavailable ({e}); retrying in 15s")
                try:
                    if self.consumer:
                        await self.consumer.stop()
                except Exception:
                    pass
                self.consumer = None
                await asyncio.sleep(15)

    async def _consume_loop(self) -> None:
        while self.running:
            messages = await self.consumer.getmany(timeout_ms=1000)
            for tp, records in messages.items():
                for record in records:
                    payload = _loads(record.value)
                    if payload is not None:
                        await self._process_message(tp.topic, payload)

    async def _process_message(self, topic: str, message: Dict[str, Any]) -> None:
        try:
            if topic in TRIGGER_TOPICS:
                account_id = message.get("account_id")
                target_date = message.get("date")
                if account_id and target_date:
                    await self.summary_generator.generate_daily_summary(
                        account_id, date.fromisoformat(target_date)
                    )
                    logger.info(f"Generated insight for {account_id} on {target_date}")
            elif topic == settings.kafka_transactions_topic:
                await self._store_transaction(message)
            elif topic == settings.kafka_fraud_alerts_topic:
                await self._store_alert(message)
        except Exception as e:
            logger.error(f"Failed to process message from {topic}: {e}")

    async def _store_transaction(self, tx: Dict[str, Any]) -> None:
        tx_id = tx.get("id") or tx.get("reference")
        account_id = tx.get("account_id") or tx.get("accountId")
        if not (self.db and tx_id and account_id and tx.get("amount") is not None):
            return
        async with self.db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO transactions (id, account_id, company_id, amount, currency, type, category,
                                          vendor_id, vendor_name, description, transaction_date, status)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (id) DO NOTHING
                """,
                str(tx_id), str(account_id), tx.get("company_id"), float(tx["amount"]),
                tx.get("currency"), tx.get("type"), tx.get("category"), tx.get("vendor_id"),
                tx.get("vendor_name"), tx.get("description"),
                _ts(tx.get("transaction_date") or tx.get("timestamp")),
                tx.get("status") or "COMPLETED",
            )

    async def _store_alert(self, alert: Dict[str, Any]) -> None:
        if not (self.db and alert.get("id") and alert.get("account_id")):
            return
        async with self.db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fraud_alerts (id, transaction_id, account_id, company_id, amount,
                                          risk_score, risk_level, fraud_type, description, timestamp)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (id) DO NOTHING
                """,
                str(alert["id"]), alert.get("transaction_id"), str(alert["account_id"]),
                alert.get("company_id"), alert.get("amount"), alert.get("risk_score"),
                alert.get("risk_level"), alert.get("fraud_type"), alert.get("description"),
                _ts(alert.get("detected_at") or alert.get("created_at")),
            )

    async def stop(self) -> None:
        self.running = False
        if self.consumer:
            try:
                await self.consumer.stop()
                logger.info("Insight consumer stopped")
            except Exception as e:
                logger.error(f"Error stopping insight consumer: {e}")
