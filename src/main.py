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
from src.kafka.producer import KafkaProducer
from src.kafka.consumer import InsightConsumer
from src.models.insight import DailyInsight, InsightType, InsightPriority, InsightStatus
from src.utils.logging import setup_logging, get_logger
from src.utils.metrics import setup_metrics, track_request
from src.utils.cache import cache_response
from src.utils.auth import verify_api_key
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
kafka_producer: Optional[KafkaProducer] = None
summary_generator: Optional[DailySummaryGenerator] = None
kafka_consumer: Optional[InsightConsumer] = None
openai_client: Optional[openai.AsyncOpenAI] = None

# Constants
CACHE_TTL_SECONDS = 300  # 5 minutes
DEFAULT_LIMIT = 50
MAX_LIMIT = 100

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager with comprehensive startup/shutdown handling.
    """
    global redis_client, db_pool, kafka_producer, summary_generator, kafka_consumer, openai_client
    
    logger.info("=" * 50)
    logger.info("Starting Insight Engine Service")
    logger.info("=" * 50)
    
    start_time = datetime.utcnow()
    
    try:
        # Initialize Redis
        logger.info("Connecting to Redis...")
        redis_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password,
            db=settings.redis_db,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30
        )
        await redis_client.ping()
        logger.info("✅ Connected to Redis", extra={"host": settings.redis_host, "port": settings.redis_port})
        
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
            command_timeout=60,
            max_queries=50000,
            max_inactive_connection_lifetime=300,
            server_settings={
                'timezone': 'UTC',
                'application_name': 'insight-engine'
            }
        )
        
        # Test connection
        async with db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        logger.info("✅ Connected to PostgreSQL", extra={"database": settings.postgres_db})
        
        # Initialize Kafka producer
        logger.info("Connecting to Kafka...")
        kafka_producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id="insight-engine",
            acks="all",
            retries=3,
            compression_type="snappy"
        )
        await kafka_producer.start()
        logger.info("✅ Connected to Kafka", extra={"servers": settings.kafka_bootstrap_servers})
        
        # Initialize OpenAI if configured
        if settings.openai_api_key:
            logger.info("Initializing OpenAI client...")
            openai_client = openai.AsyncOpenAI(
                api_key=settings.openai_api_key,
                timeout=30.0,
                max_retries=3
            )
            logger.info("✅ OpenAI client initialized")
        else:
            logger.warning("⚠️ OpenAI API key not configured - AI summaries disabled")
        
        # Initialize generator
        logger.info("Initializing Daily Summary Generator...")
        summary_generator = DailySummaryGenerator(
            redis_client, db_pool, kafka_producer, openai_client
        )
        logger.info("✅ Daily Summary Generator initialized")
        
        # Start Kafka consumer
        logger.info("Starting Kafka consumer...")
        kafka_consumer = InsightConsumer(summary_generator)
        asyncio.create_task(kafka_consumer.start())
        logger.info("✅ Kafka consumer started")
        
        # Create database tables if needed
        await ensure_database_tables()
        
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
        # Shutdown
        logger.info("=" * 50)
        logger.info("Shutting down Insight Engine...")
        logger.info("=" * 50)
        
        shutdown_start = datetime.utcnow()
        
        # Stop consumer
        if kafka_consumer:
            await kafka_consumer.stop()
            logger.info("✅ Kafka consumer stopped")
        
        # Stop producer
        if kafka_producer:
            await kafka_producer.stop()
            logger.info("✅ Kafka producer stopped")
        
        # Close database pool
        if db_pool:
            await db_pool.close()
            logger.info("✅ PostgreSQL connection pool closed")
        
        # Close Redis
        if redis_client:
            await redis_client.close()
            logger.info("✅ Redis connection closed")
        
        shutdown_duration = (datetime.utcnow() - shutdown_start).total_seconds()
        logger.info(f"✅ Shutdown completed in {shutdown_duration:.2f}s")

# Create FastAPI app
app = FastAPI(
    title="CEO OS Insight Engine",
    description="""
    Advanced AI-powered insight engine providing daily business summaries,
    trend analysis, and intelligent recommendations for CEOs and business leaders.
    
    ## Features
    
    * **Daily Insights** - AI-generated daily business summaries
    * **Trend Analysis** - Automated trend detection and alerts
    * **Risk Identification** - Early warning system for business risks
    * **Opportunity Detection** - AI-powered opportunity identification
    * **Recommendations** - Actionable business recommendations
    * **Multi-account Support** - Insights for multiple business entities
    
    ## Technology Stack
    
    * **FastAPI** - High-performance async API framework
    * **PostgreSQL** - Primary data store
    * **Redis** - Caching and rate limiting
    * **Kafka** - Event streaming
    * **OpenAI** - AI-powered natural language generation
    
    ## Authentication
    
    All endpoints require an API key passed in the `X-API-Key` header.
    """,
    version=settings.service_version or "2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "CEO OS Support",
        "email": "support@ceoos.com",
        "url": "https://ceoos.com"
    },
    license_info={
        "name": "Proprietary",
        "url": "https://ceoos.com/license"
    }
)

# Add middlewares
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID", "X-Response-Time"]
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Add custom middlewares
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add processing time and request ID to response headers."""
    start_time = datetime.utcnow()
    request_id = str(uuid.uuid4())
    
    # Add request ID to request state for logging
    request.state.request_id = request_id
    
    response = await call_next(request)
    
    process_time = (datetime.utcnow() - start_time).total_seconds() * 1000
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Response-Time"] = str(int(process_time))
    
    # Track metrics
    metrics.http_requests_total.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    
    metrics.http_request_duration_seconds.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(process_time / 1000)
    
    return response

# Add exception handlers
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
    logger.error(
        f"Unhandled exception: {exc}",
        exc_info=True,
        extra={
            "request_id": getattr(request.state, "request_id", None),
            "path": request.url.path
        }
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path,
            "request_id": getattr(request.state, "request_id", None)
        }
    )

# ============================================================================
# Health & Monitoring Endpoints
# ============================================================================

@app.get("/health", tags=["System"])
async def health_check():
    """
    Comprehensive health check endpoint for container orchestration.
    Returns detailed status of all dependencies.
    """
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
        health_status["dependencies"]["redis"] = {
            "status": "disconnected",
            "error": str(e)
        }
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
        health_status["dependencies"]["postgresql"] = {
            "status": "disconnected",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check Kafka
    try:
        if kafka_producer and kafka_producer.producer:
            health_status["dependencies"]["kafka"] = {
                "status": "connected",
                "brokers": settings.kafka_bootstrap_servers
            }
    except Exception as e:
        health_status["dependencies"]["kafka"] = {
            "status": "disconnected",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check OpenAI
    if openai_client:
        health_status["dependencies"]["openai"] = {"status": "configured"}
    else:
        health_status["dependencies"]["openai"] = {"status": "not_configured"}
    
    # Add uptime if available
    if hasattr(app, "startup_time"):
        health_status["uptime_seconds"] = (datetime.utcnow() - app.startup_time).total_seconds()
    
    return health_status

@app.get("/ready", tags=["System"])
async def ready_check():
    """Readiness probe for Kubernetes."""
    return {
        "status": "ready",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/metrics", tags=["System"])
async def get_metrics():
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

# ============================================================================
# Insight Endpoints
# ============================================================================

@app.get(
    "/insights/daily/{account_id}",
    response_model=DailyInsight,
    tags=["Insights"],
    summary="Get daily insight for an account"
)
@track_request
@cache_response(ttl_seconds=CACHE_TTL_SECONDS)
async def get_daily_insight(
    request: Request,
    account_id: str,
    insight_date: Optional[date] = Query(
        None,
        description="Date for insight (defaults to yesterday)"
    ),
    api_key: str = Depends(verify_api_key)
):
    """
    Retrieve a specific daily insight for an account.
    
    - **account_id**: Account identifier
    - **insight_date**: Date for insight (YYYY-MM-DD format)
    
    Returns the complete insight including metrics, events, and recommendations.
    """
    if not insight_date:
        insight_date = date.today() - timedelta(days=1)
    
    logger.info(
        f"Fetching daily insight for account {account_id} on {insight_date}",
        extra={
            "account_id": account_id,
            "date": insight_date.isoformat(),
            "request_id": getattr(request.state, "request_id", None)
        }
    )
    
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT * FROM daily_insights
            WHERE account_id = $1 AND date = $2
        """, account_id, insight_date)
        
        if not row:
            logger.warning(
                f"No insight found for {account_id} on {insight_date}",
                extra={
                    "account_id": account_id,
                    "date": insight_date.isoformat()
                }
            )
            raise HTTPException(
                status_code=404,
                detail=f"No insight found for {account_id} on {insight_date}"
            )
        
        insight = DailyInsight(**dict(row))
        
        # Track metrics
        metrics.insights_retrieved.labels(
            account_id=account_id[:8],
            insight_type="daily"
        ).inc()
        
        return insight

@app.get(
    "/insights/daily/{account_id}/latest",
    response_model=DailyInsight,
    tags=["Insights"],
    summary="Get latest daily insight"
)
@track_request
@cache_response(ttl_seconds=CACHE_TTL_SECONDS // 2)
async def get_latest_daily_insight(
    request: Request,
    account_id: str,
    api_key: str = Depends(verify_api_key)
):
    """
    Get the most recent daily insight for an account.
    
    - **account_id**: Account identifier
    
    Returns the latest available insight.
    """
    logger.info(
        f"Fetching latest insight for account {account_id}",
        extra={
            "account_id": account_id,
            "request_id": getattr(request.state, "request_id", None)
        }
    )
    
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT * FROM daily_insights
            WHERE account_id = $1
            ORDER BY date DESC
            LIMIT 1
        """, account_id)
        
        if not row:
            logger.warning(f"No insights found for {account_id}")
            raise HTTPException(
                status_code=404,
                detail=f"No insights found for {account_id}"
            )
        
        return DailyInsight(**dict(row))

@app.post(
    "/insights/daily/generate/{account_id}",
    tags=["Insights"],
    summary="Generate daily insight",
    status_code=202
)
@track_request
async def generate_daily_insight(
    request: Request,
    account_id: str,
    background_tasks: BackgroundTasks,
    force: bool = Query(
        False,
        description="Force regeneration even if exists"
    ),
    insight_date: Optional[date] = Query(
        None,
        description="Date to generate insight for"
    ),
    api_key: str = Depends(verify_api_key)
):
    """
    Manually trigger daily insight generation for an account.
    
    - **account_id**: Account identifier
    - **force**: If true, regenerate even if insight exists
    - **insight_date**: Optional date to generate for (defaults to yesterday)
    
    Returns immediately with a 202 Accepted status. Generation runs in background.
    """
    target_date = insight_date or (date.today() - timedelta(days=1))
    
    logger.info(
        f"Triggering insight generation for account {account_id} on {target_date}",
        extra={
            "account_id": account_id,
            "date": target_date.isoformat(),
            "force": force,
            "request_id": getattr(request.state, "request_id", None)
        }
    )
    
    # Check if already exists
    if not force:
        async with db_pool.acquire() as conn:
            exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM daily_insights
                    WHERE account_id = $1 AND date = $2
                )
            """, account_id, target_date)
            
            if exists:
                raise HTTPException(
                    status_code=409,
                    detail=f"Insight already exists for {account_id} on {target_date}. Use force=true to regenerate."
                )
    
    # Add to background tasks
    background_tasks.add_task(
        generate_insight_task,
        account_id,
        target_date,
        force
    )
    
    metrics.insight_generations_triggered.labels(
        account_id=account_id[:8],
        force=str(force)
    ).inc()
    
    return {
        "status": "accepted",
        "message": "Insight generation started",
        "account_id": account_id,
        "date": target_date.isoformat(),
        "estimated_completion": (datetime.utcnow() + timedelta(seconds=30)).isoformat()
    }

async def generate_insight_task(
    account_id: str,
    target_date: date,
    force: bool
):
    """Background task for insight generation."""
    logger.info(
        f"Starting insight generation for {account_id} on {target_date}",
        extra={
            "account_id": account_id,
            "date": target_date.isoformat(),
            "force": force
        }
    )
    
    start_time = datetime.utcnow()
    
    try:
        # Generate insight
        insight = await summary_generator.generate_daily_summary(
            account_id,
            target_date
        )
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        if insight:
            logger.info(
                f"Insight generated successfully for {account_id} on {target_date}",
                extra={
                    "account_id": account_id,
                    "date": target_date.isoformat(),
                    "insight_id": insight.id,
                    "duration_seconds": round(duration, 2),
                    "metrics_count": len(insight.metrics),
                    "recommendations_count": len(insight.recommendations)
                }
            )
            
            # Track metrics
            metrics.insight_generation_duration.observe(duration)
            metrics.insights_generated.labels(
                account_id=account_id[:8],
                status="success"
            ).inc()
            
        else:
            logger.error(
                f"Failed to generate insight for {account_id} on {target_date}",
                extra={
                    "account_id": account_id,
                    "date": target_date.isoformat()
                }
            )
            
            metrics.insights_generated.labels(
                account_id=account_id[:8],
                status="failed"
            ).inc()
            
    except Exception as e:
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.error(
            f"Error generating insight: {e}",
            exc_info=True,
            extra={
                "account_id": account_id,
                "date": target_date.isoformat()
            }
        )
        
        metrics.insights_generated.labels(
            account_id=account_id[:8],
            status="error"
        ).inc()

# ============================================================================
# Batch & Query Endpoints
# ============================================================================

@app.get(
    "/insights/recent",
    response_model=List[DailyInsight],
    tags=["Insights"],
    summary="Get recent insights across accounts"
)
@track_request
@cache_response(ttl_seconds=CACHE_TTL_SECONDS)
async def get_recent_insights(
    request: Request,
    limit: int = Query(
        DEFAULT_LIMIT,
        ge=1,
        le=MAX_LIMIT,
        description="Number of insights to return"
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Offset for pagination"
    ),
    account_id: Optional[str] = Query(
        None,
        description="Filter by account ID"
    ),
    insight_type: Optional[InsightType] = Query(
        None,
        description="Filter by insight type"
    ),
    min_confidence: Optional[float] = Query(
        None,
        ge=0,
        le=1,
        description="Minimum confidence score"
    ),
    start_date: Optional[date] = Query(
        None,
        description="Start date filter"
    ),
    end_date: Optional[date] = Query(
        None,
        description="End date filter"
    ),
    api_key: str = Depends(verify_api_key)
):
    """
    Get recent insights with optional filtering.
    
    - **limit**: Maximum number of insights to return (max 100)
    - **offset**: Pagination offset
    - **account_id**: Filter by specific account
    - **insight_type**: Filter by insight type
    - **min_confidence**: Minimum confidence score (0-1)
    - **start_date/end_date**: Date range filter
    """
    logger.info(
        f"Fetching recent insights",
        extra={
            "limit": limit,
            "offset": offset,
            "account_id": account_id,
            "request_id": getattr(request.state, "request_id", None)
        }
    )
    
    async with db_pool.acquire() as conn:
        # Build query with filters
        query = "SELECT * FROM daily_insights WHERE 1=1"
        params = []
        param_idx = 1
        
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
        
        insights = [DailyInsight(**dict(row)) for row in rows]
        
        logger.info(f"Retrieved {len(insights)} insights")
        
        return insights

@app.get(
    "/insights/accounts/{account_id}/summary",
    tags=["Insights"],
    summary="Get account insight summary"
)
@track_request
@cache_response(ttl_seconds=CACHE_TTL_SECONDS)
async def get_account_insight_summary(
    request: Request,
    account_id: str,
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get summary of insights for an account over a period.
    
    - **account_id**: Account identifier
    - **days**: Number of days to analyze (max 365)
    """
    logger.info(
        f"Generating insight summary for {account_id} over {days} days",
        extra={
            "account_id": account_id,
            "days": days,
            "request_id": getattr(request.state, "request_id", None)
        }
    )
    
    cutoff_date = date.today() - timedelta(days=days)
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                COUNT(*) as total_insights,
                AVG(confidence) as avg_confidence,
                AVG(sentiment) as avg_sentiment,
                jsonb_agg(metrics) as all_metrics,
                jsonb_agg(recommendations) as all_recommendations
            FROM daily_insights
            WHERE account_id = $1 AND date >= $2
        """, account_id, cutoff_date)
        
        if not rows or not rows[0]['total_insights']:
            return {
                "account_id": account_id,
                "days": days,
                "total_insights": 0,
                "message": "No insights found for this period"
            }
        
        row = rows[0]
        
        # Calculate recommendation counts by priority
        priority_counts = {p.value: 0 for p in InsightPriority}
        recommendations = row['all_recommendations'] or []
        
        for day_recs in recommendations:
            if isinstance(day_recs, list):
                for rec in day_recs:
                    if isinstance(rec, dict) and 'priority' in rec:
                        priority = rec['priority'].upper()
                        if priority in priority_counts:
                            priority_counts[priority] += 1
        
        return {
            "account_id": account_id,
            "period_days": days,
            "start_date": cutoff_date.isoformat(),
            "end_date": date.today().isoformat(),
            "total_insights": row['total_insights'],
            "avg_confidence": round(float(row['avg_confidence']), 3) if row['avg_confidence'] else None,
            "avg_sentiment": round(float(row['avg_sentiment']), 3) if row['avg_sentiment'] else None,
            "recommendations_by_priority": priority_counts,
            "insights_per_day": await get_insights_per_day(conn, account_id, cutoff_date)
        }

async def get_insights_per_day(conn, account_id: str, cutoff_date: date) -> Dict[str, int]:
    """Get count of insights per day."""
    rows = await conn.fetch("""
        SELECT date::text, COUNT(*)
        FROM daily_insights
        WHERE account_id = $1 AND date >= $2
        GROUP BY date
        ORDER BY date DESC
    """, account_id, cutoff_date)
    
    return {row['date']: row['count'] for row in rows}

# ============================================================================
# Data Management Endpoints
# ============================================================================

@app.delete(
    "/insights/accounts/{account_id}",
    tags=["Admin"],
    summary="Delete all insights for an account"
)
async def delete_account_insights(
    request: Request,
    account_id: str,
    confirmation: bool = Query(False, description="Confirmation required"),
    api_key: str = Depends(verify_api_key)
):
    """
    Delete all insights for an account (admin only).
    
    - **account_id**: Account identifier
    - **confirmation**: Must be true to confirm deletion
    """
    if not confirmation:
        raise HTTPException(
            status_code=400,
            detail="Confirmation required. Set confirmation=true to proceed."
        )
    
    logger.warning(
        f"Deleting all insights for account {account_id}",
        extra={
            "account_id": account_id,
            "request_id": getattr(request.state, "request_id", None)
        }
    )
    
    async with db_pool.acquire() as conn:
        result = await conn.execute("""
            DELETE FROM daily_insights
            WHERE account_id = $1
        """, account_id)
        
        # Clear cache
        cache_pattern = f"insight:{account_id}:*"
        await redis_client.delete_pattern(cache_pattern)
    
    return {
        "status": "success",
        "message": f"All insights deleted for account {account_id}"
    }

# ============================================================================
# Utility Functions
# ============================================================================

async def ensure_database_tables():
    """Ensure required database tables exist."""
    async with db_pool.acquire() as conn:
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
                UNIQUE(account_id, date)
            )
        """)
        
        # Create indexes
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_insights_account ON daily_insights(account_id);
            CREATE INDEX IF NOT EXISTS idx_insights_date ON daily_insights(date);
            CREATE INDEX IF NOT EXISTS idx_insights_account_date ON daily_insights(account_id, date);
            CREATE INDEX IF NOT EXISTS idx_insights_type ON daily_insights(type);
            CREATE INDEX IF NOT EXISTS idx_insights_status ON daily_insights(status);
            CREATE INDEX IF NOT EXISTS idx_insights_generated ON daily_insights(generated_at);
        """)
        
        logger.info("✅ Database tables verified")

async def warmup_caches():
    """Warm up caches with frequently accessed data."""
    try:
        logger.info("Starting cache warmup...")
        
        # Get most active accounts
        async with db_pool.acquire() as conn:
            accounts = await conn.fetch("""
                SELECT DISTINCT account_id
                FROM daily_insights
                ORDER BY generated_at DESC
                LIMIT 10
            """)
        
        # Warm up latest insights for active accounts
        for account in accounts:
            cache_key = f"insight:{account['account_id']}:latest"
            await redis_client.delete(cache_key)  # Clear stale cache
            
        logger.info(f"✅ Cache warmup completed for {len(accounts)} accounts")
        
    except Exception as e:
        logger.error(f"Cache warmup failed: {e}")

# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print(f"🚀 Starting {settings.service_name} v{settings.service_version}")
    print(f"📍 Environment: {settings.environment}")
    print(f"🌐 Server will run at: http://0.0.0.0:8000")
    print(f"📚 API Docs: http://0.0.0.0:8000/docs")
    print(f"🔍 Health Check: http://0.0.0.0:8000/health")
    print("=" * 60)
    
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.environment == "development",
        log_level="info"
    )

# Export app
__all__ = ["app"]