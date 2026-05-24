"""
Kafka consumer for the Insight Engine.
"""

import asyncio
import json
import logging
from typing import Optional, Any
from aiokafka import AIOKafkaConsumer

from src.config import settings

logger = logging.getLogger(__name__)


class InsightConsumer:
    """
    Async Kafka consumer for the Insight Engine.
    Listens for events that trigger insight generation.
    """

    def __init__(self, summary_generator: Any):
        self.summary_generator = summary_generator
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False

    async def start(self) -> None:
        """Start the Kafka consumer."""
        try:
            self.consumer = AIOKafkaConsumer(
                "insight-requests",
                "daily-summary-trigger",
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id="insight-engine-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            await self.consumer.start()
            self.running = True
            logger.info("Insight Kafka consumer started")
            await self._consume_loop()
        except Exception as e:
            logger.error(f"Insight consumer failed to start: {e}")
            self.running = False

    async def _consume_loop(self) -> None:
        """Main consumption loop."""
        while self.running:
            try:
                messages = await self.consumer.getmany(timeout_ms=1000)
                for tp, records in messages.items():
                    for record in records:
                        await self._process_message(record.value)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in insight consume loop: {e}")
                await asyncio.sleep(1)

    async def _process_message(self, message: Any) -> None:
        """Process a single message."""
        try:
            account_id = message.get('account_id')
            target_date = message.get('date')

            if account_id and target_date:
                from datetime import date
                parsed_date = date.fromisoformat(target_date)
                await self.summary_generator.generate_daily_summary(account_id, parsed_date)
                logger.info(f"Generated insight for {account_id} on {parsed_date}")
        except Exception as e:
            logger.error(f"Failed to process insight message: {e}")

    async def stop(self) -> None:
        """Stop the consumer."""
        self.running = False
        if self.consumer:
            try:
                await self.consumer.stop()
                logger.info("Insight consumer stopped")
            except Exception as e:
                logger.error(f"Error stopping insight consumer: {e}")
