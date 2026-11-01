"""
Production-grade Insight Engine API for AI-powered daily business summaries,
trend analysis, and intelligent recommendations.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, Response
from contextlib import asynccontextmanager
import asyncpg
import redis.asyncio as redis
import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
import uuid
import json
import asyncio
import openai
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.config import settings
from src.generators.daily_summary import DailySummaryGenerator
from src.kafka.consumer import make_handlers, TRIGGER_TOPICS
from src.kafka.outbox import OUTBOX_DDL, OutboxProducer
from src.kafka.reliable_consumer import ReliableConsumer, connection_kwargs
from src.utils.tenant import TENANT_SQL, setting_value, use_tenant
from src.models.insight import DailyInsight, InsightType, InsightPriority, InsightStatus
from src.utils.logging import setup_logging, get_logger
from src.utils.metrics import setup_metrics, track_request
from src.utils.cache import cache_response
from src.utils.auth import ALL_COMPANIES, company_of, verify_api_key
from src.utils.exceptions import (
    AppException,
    not_found_handler,
    validation_exception_handler,
    app_exception_handler
)

# Setup logging and metrics
setup_logging()
logger = get_logger(__name__)
metrics = setup_metrics()

# Global objects
redis_client: Optional[redis.Redis] = None
db_pool: Optional[asyncpg.Pool] = None
kafka_producer: Optional[OutboxProducer] = None
summary_generator: Optional[DailySummaryGenerator] = None
kafka_consumer: Optional[ReliableConsumer] = None
openai_client: Optional[openai.AsyncOpenAI] = None

# Constants
CACHE_TTL_SECONDS = 300  # 5 minutes
DEFAULT_LIMIT = 50
MAX_LIMIT = 100


async def _init_db_connection(conn) -> None:
    """Decode JSON/JSONB to Python objects (asyncpg returns str by default)."""
    for typ in ("json", "jsonb"):
        await conn.set_type_codec(
            typ,
            encoder=lambda v: v if isinstance(v, str) else json.dumps(v, default=str),
            decoder=json.loads,
            schema="pg_catalog",
        )


async def _tag_tenant(conn) -> None:
    await conn.execute("SELECT set_config('ceoos.tenant', $1, false)", setting_value())


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, db_pool, kafka_producer, summary_generator, kafka_consumer, openai_client

    logger.info("=" * 50)
    logger.info("Starting Insight Engine Service")
    logger.info("=" * 50)

    start_time = datetime.utcnow()

    try:
        # Initialize Redis
        logger.info("Connecting to Redis...")
        common = dict(password=settings.redis_password or None, decode_responses=True, ssl=settings.redis_tls,
                      socket_timeout=5, socket_connect_timeout=5, health_check_interval=30)
        if settings.redis_cluster:
            from redis.asyncio.cluster import RedisCluster
            redis_client = RedisCluster(host=settings.redis_host, port=settings.redis_port, **common)
        else:
            redis_client = redis.Redis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db,
                                       retry_on_timeout=True, **common)
        try:
            await redis_client.ping()
            logger.info("✅ Connected to Redis")
        except Exception as e:  # /ready reports it; don't crash-loop
            logger.error(f"Redis unavailable at startup: {e}")

        # Initialize PostgreSQL
        logger.info("Connecting to PostgreSQL...")
        db_pool = await asyncpg.create_pool(
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
            min_size=settings.db_pool_min_size or 5,
            max_size=settings.db_pool_max_size or 20,
            command_timeout=settings.db_statement_timeout_ms / 1000,
            max_queries=50000,
            max_inactive_connection_lifetime=300,
            server_settings={
                'timezone': 'UTC',
                'application_name': 'insight-engine',
                'statement_timeout': str(settings.db_statement_timeout_ms),
            },
            ssl="require" if settings.postgres_ssl else None,
            init=_init_db_connection,
            # Tag every checkout with the caller's tenant (see utils/tenant.py).
            setup=_tag_tenant,
        )
        async with db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        logger.info("✅ Connected to PostgreSQL")

        if settings.run_migrations:
            await ensure_database_tables()

        # Outbox-backed producer: messages are durable in Postgres and relayed
        # to Kafka whenever it is reachable.
        kafka_conn = connection_kwargs(settings.kafka_bootstrap_servers, settings.kafka_ssl,
                                       settings.kafka_sasl_mechanism, settings.kafka_sasl_username,
                                       settings.kafka_sasl_password)
        kafka_producer = OutboxProducer(db_pool, kafka_conn, client_id="insight-engine")
        kafka_producer.start()

        # Initialize OpenAI if configured
        if settings.openai_api_key:
            logger.info("Initializing OpenAI client...")
            try:
                openai_client = openai.AsyncOpenAI(
                    api_key=settings.openai_api_key,
                    timeout=30.0,
                    max_retries=3
                )
                logger.info("✅ OpenAI client initialized")
            except Exception as e:
                logger.warning(f"⚠️ OpenAI initialization failed: {e}")
                openai_client = None
        else:
            logger.warning("⚠️ OpenAI API key not configured - AI summaries disabled")

        # Initialize generator
        logger.info("Initializing Daily Summary Generator...")
        summary_generator = DailySummaryGenerator(
            redis_client, db_pool, kafka_producer, openai_client
        )
        logger.info("✅ Daily Summary Generator initialized")

        # At-least-once consumer with retries + DLQ; reconnects on its own.
        kafka_consumer = ReliableConsumer(
            topics=[*TRIGGER_TOPICS, settings.kafka_transactions_topic, settings.kafka_fraud_alerts_topic],
            group_id=settings.kafka_group_id,
            handlers=make_handlers(summary_generator, db_pool),
            conn=kafka_conn,
            client_id="insight-engine",
        )
        kafka_consumer.start()

        # Warm up caches
        asyncio.create_task(warmup_caches())

        startup_duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info("=" * 50)
        logger.info(f"✅ Insight Engine started successfully in {startup_duration:.2f}s")
        logger.info("=" * 50)

        yield

    except Exception as e:
        logger.error("❌ Failed to start Insight Engine", exc_info=True)
        raise

    finally:
        logger.info("=" * 50)
        logger.info("Shutting down Insight Engine...")
        logger.info("=" * 50)

        shutdown_start = datetime.utcnow()

        if kafka_consumer:
            try:
                await kafka_consumer.stop()
                logger.info("✅ Kafka consumer stopped")
            except Exception:
                pass

        if kafka_producer:
            try:
                await kafka_producer.stop()
                logger.info("✅ Kafka producer stopped")
            except Exception:
                pass

        if db_pool:
            await db_pool.close()
            logger.info("✅ PostgreSQL connection pool closed")

        if redis_client:
            await redis_client.close()
            logger.info("✅ Redis connection closed")

        shutdown_duration = (datetime.utcnow() - shutdown_start).total_seconds()
        logger.info(f"✅ Shutdown completed in {shutdown_duration:.2f}s")


# Create FastAPI app
app = FastAPI(
    title="CEO OS Insight Engine",
    description="Advanced AI-powered insight engine providing daily business summaries, trend analysis, and intelligent recommendations.",
    version=settings.service_version or "2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID", "X-Response-Time"]
)
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = datetime.utcnow()
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    process_time = (datetime.utcnow() - start_time).total_seconds() * 1000
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Response-Time"] = str(int(process_time))
    try:
        metrics.http_requests_total.labels(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code
        ).inc()
        metrics.http_request_duration_seconds.labels(
            method=request.method,
            endpoint=request.url.path
        ).observe(process_time / 1000)
    except Exception:
        pass
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path,
            "request_id": getattr(request.state, "request_id", None)
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path,
            "request_id": getattr(request.state, "request_id", None)
        }
    )


def row_to_insight(row) -> DailyInsight:
    """Build the API model from a DB row, tolerating NULLs and non-finite metrics."""
    data = {k: v for k, v in dict(row).items() if v is not None and k != "company_id"}
    metrics = data.get("metrics") or {}
    # Non-finite values (e.g. infinite runway) are stored as null; omit them.
    data["metrics"] = {
        name: m for name, m in metrics.items() if isinstance(m, dict) and m.get("value") is not None
    }
    return DailyInsight(**data)


# ============================================================================
# Health & Monitoring Endpoints
# ============================================================================

@app.get("/health", tags=["System"])
async def health_check():
    health_status = {
        "status": "healthy",
        "service": "insight-engine",
        "version": settings.service_version,
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": {}
    }

    # Check Redis
    try:
        await redis_client.ping()
        info = await redis_client.info("server")
        health_status["dependencies"]["redis"] = {
            "status": "connected",
            "version": info.get("redis_version", "unknown")
        }
    except Exception as e:
        health_status["dependencies"]["redis"] = {"status": "disconnected", "error": str(e)}
        health_status["status"] = "degraded"

    # Check PostgreSQL
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
            version = await conn.fetchval("SHOW server_version")
        health_status["dependencies"]["postgresql"] = {
            "status": "connected",
            "version": version
        }
    except Exception as e:
        health_status["dependencies"]["postgresql"] = {"status": "disconnected", "error": str(e)}
        health_status["status"] = "degraded"

    # Check Kafka (non-fatal)
    if kafka_producer and kafka_producer.connected:
        health_status["dependencies"]["kafka"] = {
            "status": "connected",
            "brokers": settings.kafka_bootstrap_servers
        }
    else:
        health_status["dependencies"]["kafka"] = {"status": "not_connected"}

    # Check OpenAI
    health_status["dependencies"]["openai"] = {
        "status": "configured" if openai_client else "not_configured"
    }

    return health_status


@app.get("/ready", tags=["System"])
async def ready_check():
    """Take traffic only when Postgres and Redis answer."""
    try:
        async with db_pool.acquire(timeout=2) as conn:
            await conn.fetchval("SELECT 1")
        await asyncio.wait_for(redis_client.ping(), 2)
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "not_ready", "error": type(e).__name__})
    return {"status": "ready", "timestamp": datetime.utcnow().isoformat()}


@app.get("/metrics", tags=["System"])
async def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ============================================================================
# Insight Endpoints
# ============================================================================

@app.get("/insights/daily/{account_id}", response_model=DailyInsight, tags=["Insights"])
@track_request
@cache_response(ttl_seconds=CACHE_TTL_SECONDS)
async def get_daily_insight(
    request: Request,
    account_id: str,
    insight_date: Optional[date] = Query(None, description="Date for insight (defaults to yesterday)"),
    api_key: str = Depends(verify_api_key)
):
    if not insight_date:
        insight_date = date.today() - timedelta(days=1)

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM daily_insights WHERE account_id = $1 AND date = $2"
            " AND ($3 = '*' OR company_id = $3)",
            account_id, insight_date, api_key
        )
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"No insight found for {account_id} on {insight_date}"
            )
        try:
            metrics.insights_retrieved.labels(account_id=account_id[:8], insight_type="daily").inc()
        except Exception:
            pass
        return row_to_insight(row)


@app.get("/insights/daily/{account_id}/latest", response_model=DailyInsight, tags=["Insights"])
@track_request
@cache_response(ttl_seconds=CACHE_TTL_SECONDS // 2)
async def get_latest_daily_insight(
    request: Request,
    account_id: str,
    api_key: str = Depends(verify_api_key)
):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM daily_insights WHERE account_id = $1"
            " AND ($2 = '*' OR company_id = $2) ORDER BY date DESC LIMIT 1",
            account_id, api_key
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"No insights found for {account_id}")
        return row_to_insight(row)


@app.post("/insights/daily/generate/{account_id}", tags=["Insights"], status_code=202)
@track_request
async def generate_daily_insight(
    request: Request,
    account_id: str,
    background_tasks: BackgroundTasks,
    force: bool = Query(False, description="Force regeneration even if exists"),
    insight_date: Optional[date] = Query(None, description="Date to generate insight for"),
    api_key: str = Depends(verify_api_key)
):
    target_date = insight_date or (date.today() - timedelta(days=1))

    if not force:
        async with db_pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM daily_insights WHERE account_id = $1 AND date = $2"
                " AND ($3 = '*' OR company_id = $3))",
                account_id, target_date, api_key
            )
            if exists:
                raise HTTPException(
                    status_code=409,
                    detail=f"Insight already exists for {account_id} on {target_date}. Use force=true to regenerate."
                )

    background_tasks.add_task(generate_insight_task, account_id, target_date, force, company_of(api_key))

    try:
        metrics.insight_generations_triggered.labels(
            account_id=account_id[:8], force=str(force)
        ).inc()
    except Exception:
        pass

    return {
        "status": "accepted",
        "message": "Insight generation started",
        "account_id": account_id,
        "date": target_date.isoformat(),
        "estimated_completion": (datetime.utcnow() + timedelta(seconds=30)).isoformat()
    }


async def generate_insight_task(account_id: str, target_date: date, force: bool, company_id: Optional[str] = None):
    start_time = datetime.utcnow()
    try:
        # Generate within the caller's tenant: reads only its data, stores
        # the insight under its company id.
        with use_tenant(company_id):
            insight = await summary_generator.generate_daily_summary(account_id, target_date)
        duration = (datetime.utcnow() - start_time).total_seconds()
        status = "success" if insight else "failed"
        try:
            metrics.insight_generation_duration.observe(duration)
            metrics.insights_generated.labels(account_id=account_id[:8], status=status).inc()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Error generating insight: {e}", exc_info=True)
        try:
            metrics.insights_generated.labels(account_id=account_id[:8], status="error").inc()
        except Exception:
            pass


@app.get("/insights/recent", response_model=List[DailyInsight], tags=["Insights"])
@track_request
@cache_response(ttl_seconds=CACHE_TTL_SECONDS)
async def get_recent_insights(
    request: Request,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    account_id: Optional[str] = Query(None),
    insight_type: Optional[InsightType] = Query(None),
    min_confidence: Optional[float] = Query(None, ge=0, le=1),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    api_key: str = Depends(verify_api_key)
):
    async with db_pool.acquire() as conn:
        query = "SELECT * FROM daily_insights WHERE 1=1"
        params = []
        if api_key != ALL_COMPANIES:
            params.append(api_key)
            query += " AND company_id = $1"
        param_idx = len(params) + 1

        if account_id:
            query += f" AND account_id = ${param_idx}"
            params.append(account_id)
            param_idx += 1
        if insight_type:
            query += f" AND type = ${param_idx}"
            params.append(insight_type.value)
            param_idx += 1
        if min_confidence is not None:
            query += f" AND confidence >= ${param_idx}"
            params.append(min_confidence)
            param_idx += 1
        if start_date:
            query += f" AND date >= ${param_idx}"
            params.append(start_date)
            param_idx += 1
        if end_date:
            query += f" AND date <= ${param_idx}"
            params.append(end_date)
            param_idx += 1

        query += f" ORDER BY generated_at DESC LIMIT ${param_idx} OFFSET ${param_idx+1}"
        params.extend([limit, offset])

        rows = await conn.fetch(query, *params)
        return [row_to_insight(row) for row in rows]


@app.get("/insights/accounts/{account_id}/summary", tags=["Insights"])
@track_request
@cache_response(ttl_seconds=CACHE_TTL_SECONDS)
async def get_account_insight_summary(
    request: Request,
    account_id: str,
    days: int = Query(30, ge=1, le=365),
    api_key: str = Depends(verify_api_key)
):
    cutoff_date = date.today() - timedelta(days=days)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT COUNT(*) as total_insights,
                   AVG(confidence) as avg_confidence,
                   AVG(sentiment) as avg_sentiment,
                   jsonb_agg(recommendations) as all_recommendations
            FROM daily_insights
            WHERE account_id = $1 AND date >= $2
              AND ($3 = '*' OR company_id = $3)
        """, account_id, cutoff_date, api_key)

        if not rows or not rows[0]['total_insights']:
            return {
                "account_id": account_id,
                "days": days,
                "total_insights": 0,
                "message": "No insights found for this period"
            }

        row = rows[0]
        priority_counts = {p.value: 0 for p in InsightPriority}
        for day_recs in (row['all_recommendations'] or []):
            if isinstance(day_recs, list):
                for rec in day_recs:
                    if isinstance(rec, dict) and 'priority' in rec:
                        p = rec['priority'].upper()
                        if p in priority_counts:
                            priority_counts[p] += 1

        return {
            "account_id": account_id,
            "period_days": days,
            "start_date": cutoff_date.isoformat(),
            "end_date": date.today().isoformat(),
            "total_insights": row['total_insights'],
            "avg_confidence": round(float(row['avg_confidence']), 3) if row['avg_confidence'] else None,
            "avg_sentiment": round(float(row['avg_sentiment']), 3) if row['avg_sentiment'] else None,
            "recommendations_by_priority": priority_counts,
            "insights_per_day": await get_insights_per_day(conn, account_id, cutoff_date, api_key)
        }


async def get_insights_per_day(conn, account_id: str, cutoff_date: date, scope: str = ALL_COMPANIES) -> Dict[str, int]:
    rows = await conn.fetch("""
        SELECT date::text, COUNT(*)
        FROM daily_insights
        WHERE account_id = $1 AND date >= $2
          AND ($3 = '*' OR company_id = $3)
        GROUP BY date ORDER BY date DESC
    """, account_id, cutoff_date, scope)
    return {row['date']: row['count'] for row in rows}


@app.delete("/insights/accounts/{account_id}", tags=["Admin"])
async def delete_account_insights(
    request: Request,
    account_id: str,
    confirmation: bool = Query(False),
    api_key: str = Depends(verify_api_key)
):
    if api_key != ALL_COMPANIES:
        raise HTTPException(status_code=403, detail="Admin role required")
    if not confirmation:
        raise HTTPException(
            status_code=400,
            detail="Confirmation required. Set confirmation=true to proceed."
        )

    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM daily_insights WHERE account_id = $1", account_id
        )

    return {"status": "success", "message": f"All insights deleted for account {account_id}"}


# ============================================================================
# Utility Functions
# ============================================================================

async def ensure_database_tables():
    """Idempotent schema. Advisory lock: replicas / the migration Job never race."""
    async with db_pool.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(727277)")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_insights (
                id VARCHAR(50) PRIMARY KEY,
                account_id VARCHAR(100) NOT NULL,
                date DATE NOT NULL,
                type VARCHAR(50) NOT NULL DEFAULT 'DAILY_SUMMARY',
                status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
                summary TEXT,
                executive_summary TEXT,
                metrics JSONB,
                key_events JSONB,
                recommendations JSONB,
                risks JSONB,
                opportunities JSONB,
                sentiment FLOAT,
                confidence FLOAT,
                metadata JSONB,
                tags TEXT[],
                version VARCHAR(10) DEFAULT '2.0',
                generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                period_start TIMESTAMP WITH TIME ZONE,
                period_end TIMESTAMP WITH TIME ZONE,
                company_id VARCHAR(100) NOT NULL DEFAULT '',
                UNIQUE(company_id, account_id, date)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_insights_account ON daily_insights(account_id);
            CREATE INDEX IF NOT EXISTS idx_insights_date ON daily_insights(date);
            CREATE INDEX IF NOT EXISTS idx_insights_account_date ON daily_insights(account_id, date);
            CREATE INDEX IF NOT EXISTS idx_insights_type ON daily_insights(type);
            CREATE INDEX IF NOT EXISTS idx_insights_status ON daily_insights(status);
            CREATE INDEX IF NOT EXISTS idx_insights_generated ON daily_insights(generated_at);
        """)
        await conn.execute("""
            ALTER TABLE daily_insights ADD COLUMN IF NOT EXISTS company_id VARCHAR(100);
            CREATE INDEX IF NOT EXISTS idx_insights_company ON daily_insights(company_id);

            -- Local copies of upstream data the summary generator reads.
            -- Fed from Kafka (`transactions`, `fraud-alerts`) by the Kafka handlers (src/kafka/consumer.py).
            CREATE TABLE IF NOT EXISTS transactions (
                id VARCHAR(100) PRIMARY KEY,
                account_id VARCHAR(100) NOT NULL,
                company_id VARCHAR(100),
                amount DECIMAL(15,2) NOT NULL,
                currency VARCHAR(3),
                type VARCHAR(20),
                category VARCHAR(100),
                vendor_id VARCHAR(100),
                vendor_name VARCHAR(255),
                description TEXT,
                transaction_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status VARCHAR(20) DEFAULT 'COMPLETED',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_ie_tx_account_date ON transactions(account_id, transaction_date DESC);

            CREATE TABLE IF NOT EXISTS fraud_alerts (
                id VARCHAR(50) PRIMARY KEY,
                transaction_id VARCHAR(100),
                account_id VARCHAR(100) NOT NULL,
                company_id VARCHAR(100),
                amount DECIMAL(15,2),
                risk_score DECIMAL(4,3),
                risk_level VARCHAR(20),
                fraud_type VARCHAR(50),
                description TEXT,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_ie_alerts_account_ts ON fraud_alerts(account_id, timestamp DESC);

            -- Optional business context per account (industry, goals, ...).
            CREATE TABLE IF NOT EXISTS accounts (
                account_id VARCHAR(100) PRIMARY KEY,
                company_id VARCHAR(100),
                industry VARCHAR(100),
                company_size VARCHAR(50),
                business_goals JSONB,
                risk_tolerance VARCHAR(20)
            );
        """)
        # Tenant isolation: ids/account numbers are only unique per institution.
        await conn.execute(TENANT_SQL)
        await conn.execute(OUTBOX_DDL)
        await conn.execute("""
            UPDATE transactions SET company_id = '' WHERE company_id IS NULL;
            ALTER TABLE transactions ALTER COLUMN company_id SET DEFAULT '';
            ALTER TABLE transactions ALTER COLUMN company_id SET NOT NULL;
            UPDATE daily_insights SET company_id = '' WHERE company_id IS NULL;
            ALTER TABLE daily_insights ALTER COLUMN company_id SET DEFAULT '';
            ALTER TABLE daily_insights ALTER COLUMN company_id SET NOT NULL;
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
                               WHERE t.relname = 'transactions' AND c.contype = 'p'
                                 AND array_length(c.conkey, 1) = 2) THEN
                    ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_pkey;
                    ALTER TABLE transactions ADD CONSTRAINT transactions_pkey PRIMARY KEY (company_id, id);
                END IF;
                ALTER TABLE daily_insights DROP CONSTRAINT IF EXISTS daily_insights_account_id_date_key;
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'daily_insights_tenant_account_date_key') THEN
                    ALTER TABLE daily_insights ADD CONSTRAINT daily_insights_tenant_account_date_key
                        UNIQUE (company_id, account_id, date);
                END IF;
            END $$;
            CREATE INDEX IF NOT EXISTS idx_ie_tx_company_account_date
                ON transactions(company_id, account_id, transaction_date DESC);
        """)
        logger.info("✅ Database tables verified")


async def warmup_caches():
    try:
        async with db_pool.acquire() as conn:
            accounts = await conn.fetch("""
                SELECT account_id FROM daily_insights
                GROUP BY account_id ORDER BY MAX(generated_at) DESC LIMIT 10
            """)
        for account in accounts:
            await redis_client.delete(f"insight:{account['account_id']}:latest")
        logger.info(f"✅ Cache warmup completed for {len(accounts)} accounts")
    except Exception as e:
        logger.error(f"Cache warmup failed: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        log_level="info"
    )

__all__ = ["app"]
