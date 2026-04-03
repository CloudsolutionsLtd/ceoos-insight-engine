from pydantic_settings import BaseSettings
from typing import List, Optional

class Settings(BaseSettings):
    service_name: str = "insight-engine"
    service_version: str = "2.0.0"
    environment: str = "development"
    
    # Redis (AWS MemoryDB)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: Optional[str] = None
    redis_db: int = 0
    redis_tls: bool = False
    
    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "insights"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_ssl: bool = False
    
    # OpenAI
    openai_api_key: str = ""
    
    # CORS
    cors_origins: List[str] = [
        "http://localhost:3000",
        "http://localhost:3001",
        "https://cloudsolutiontech.com.ng"
    ]
    
    # Logging
    log_level: str = "INFO"
    debug: bool = True
    
    # Service URLs
    api_gateway_url: str = "http://localhost:3000"
    auth_service_url: str = "http://localhost:3001"
    finance_service_url: str = "http://localhost:3002"
    fraud_service_url: str = "http://localhost:3003"
    
    # App settings
    app_name: str = "insight-engine"
    port: int = 8000
    host: str = "0.0.0.0"
    
    # Fraud thresholds
    fraud_auto_block_threshold: float = 0.95
    fraud_2fa_threshold: float = 0.85
    fraud_review_threshold: float = 0.70
    fraud_monitor_threshold: float = 0.50
    
    class Config:
        # Disable .env file loading to avoid parsing issues
        env_file = None
        extra = "ignore"

settings = Settings()