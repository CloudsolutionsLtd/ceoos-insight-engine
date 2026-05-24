"""
Kafka producer for the Insight Engine.
"""

import json
import logging
from typing import Optional, Any, Dict
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Async Kafka producer wrapper using aiokafka.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str = "insight-engine",
        acks: str = "all",
        retries: int = 3,
        compression_type: str = "snappy"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.acks = acks
        self.retries = retries
        self.compression_type = compression_type
        self.producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        """Start the Kafka producer."""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                acks=self.acks,
                enable_idempotence=True,
                compression_type=self.compression_type,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k
            )
            await self.producer.start()
            logger.info(f"Kafka producer started: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            self.producer = None
            raise

    async def send(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None
    ) -> None:
        """Send a message to a Kafka topic."""
        if not self.producer:
            logger.warning("Kafka producer not initialized, skipping send")
            return

        try:
            await self.producer.send_and_wait(
                topic,
                value=value,
                key=key
            )
            logger.debug(f"Message sent to topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            raise

    async def stop(self) -> None:
        """Stop the Kafka producer."""
        if self.producer:
            try:
                await self.producer.stop()
                logger.info("Kafka producer stopped")
            except Exception as e:
                logger.error(f"Error stopping Kafka producer: {e}")
            finally:
                self.producer = None
