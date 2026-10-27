from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    service_name: str = "insight-engine"
    service_version: str = "2.1.0"
    environment: str = "production"

    # Auth: same values as auth-service / api-gateway
    enable_auth: bool = True
    jwt_secret: str = ""
    internal_service_token: str = ""

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: Optional[str] = None
    redis_db: int = 0
    redis_tls: bool = False

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "insight_db"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_ssl: bool = False
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_ssl: bool = False
    kafka_group_id: str = "insight-engine-group"
    # Must match fraud-service topic names
    kafka_transactions_topic: str = "transactions"
    kafka_fraud_alerts_topic: str = "fraud-alerts"

    # OpenAI (optional; AI summaries are disabled without a key)
    openai_api_key: str = ""

    # Comma-separated browser origins (requests normally arrive via the gateway)
    cors_origins: str = "http://localhost:5173"

    # Logging
    log_level: str = "INFO"
    debug: bool = False

    app_name: str = "insight-engine"
    port: int = 3006
    host: str = "0.0.0.0"

    def get_cors_origins(self) -> List[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip() and o.strip() != "*"]


settings = Settings()
