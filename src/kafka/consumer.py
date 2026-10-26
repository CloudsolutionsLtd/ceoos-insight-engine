"""
Kafka handlers for the insight engine (run by ReliableConsumer).

Topics:
  insight-requests, daily-summary-trigger   {account_id, date, company_id} -> generate summary
  transactions (KAFKA_TRANSACTIONS_TOPIC)   transaction                    -> local copy
  fraud-alerts (KAFKA_FRAUD_ALERTS_TOPIC)   alert                          -> local copy

Handlers are idempotent (ON CONFLICT DO NOTHING / upsert), because delivery
is at-least-once. Exceptions propagate so ReliableConsumer retries and, if
they keep failing, parks the message in <topic>.dlq.
"""
import logging
from datetime import date, datetime
from typing import Any, Awaitable, Callable, Dict

from src.config import settings
from src.utils.tenant import use_tenant

logger = logging.getLogger(__name__)

TRIGGER_TOPICS = ("insight-requests", "daily-summary-trigger")


def _ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.utcnow()


def make_handlers(summary_generator: Any, db_pool: Any) -> Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]]:
    async def on_trigger(message: Dict[str, Any]) -> None:
        account_id, target_date = message.get("account_id"), message.get("date")
        if not (account_id and target_date):
            return
        with use_tenant(message.get("company_id") or ""):
            await summary_generator.generate_daily_summary(account_id, date.fromisoformat(target_date))
        logger.info(f"Generated insight for {account_id} on {target_date}")

    async def on_transaction(tx: Dict[str, Any]) -> None:
        tx_id = tx.get("id") or tx.get("reference")
        account_id = tx.get("account_id") or tx.get("accountId")
        if not (tx_id and account_id and tx.get("amount") is not None):
            return
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO transactions (id, account_id, company_id, amount, currency, type, category,
                                          vendor_id, vendor_name, description, transaction_date, status)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (company_id, id) DO NOTHING
                """,
                str(tx_id), str(account_id), tx.get("company_id") or "", float(tx["amount"]),
                tx.get("currency"), tx.get("type"), tx.get("category"), tx.get("vendor_id"),
                tx.get("vendor_name"), tx.get("description"),
                _ts(tx.get("transaction_date") or tx.get("timestamp")),
                tx.get("status") or "COMPLETED",
            )

    async def on_alert(alert: Dict[str, Any]) -> None:
        if not (alert.get("id") and alert.get("account_id")):
            return
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fraud_alerts (id, transaction_id, account_id, company_id, amount,
                                          risk_score, risk_level, fraud_type, description, timestamp)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (id) DO NOTHING
                """,
                str(alert["id"]), alert.get("transaction_id"), str(alert["account_id"]),
                alert.get("company_id") or "", alert.get("amount"), alert.get("risk_score"),
                alert.get("risk_level"), alert.get("fraud_type"), alert.get("description"),
                _ts(alert.get("detected_at") or alert.get("created_at")),
            )

    handlers = {t: on_trigger for t in TRIGGER_TOPICS}
    handlers[settings.kafka_transactions_topic] = on_transaction
    handlers[settings.kafka_fraud_alerts_topic] = on_alert
    return handlers
