"""
Advanced Daily Summary Generator that creates AI-powered business insights
for CEOs using NLP, trend analysis, and intelligent recommendations.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional, Tuple
import logging
import json
import asyncio
import uuid
from enum import Enum
from dataclasses import dataclass, field
from textblob import TextBlob
import jinja2
from jinja2 import Environment, FileSystemLoader
import aiohttp
from decimal import Decimal

from src.models.insight import (
    DailyInsight, Metric, KeyEvent, Recommendation,
    InsightType, InsightPriority, InsightStatus
)
from src.utils.logging import get_logger
from src.utils.metrics import track_generation_time
from src.utils.cache import cache_result
from src.exceptions import GenerationError, InsufficientDataError

logger = get_logger(__name__)

# Constants
MIN_DATA_POINTS = 5
HIGH_CONFIDENCE_THRESHOLD = 20
MEDIUM_CONFIDENCE_THRESHOLD = 10
LOW_CONFIDENCE_THRESHOLD = 5
SENTIMENT_WEIGHT_EVENT = 0.1
SENTIMENT_WEIGHT_METRIC = 0.1
CACHE_TTL_HOURS = 24

class RiskLevel(str, Enum):
    """Risk levels for business risks."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class OpportunityType(str, Enum):
    """Types of business opportunities."""
    GROWTH = "growth"
    COST_OPTIMIZATION = "cost_optimization"
    VENDOR_DIVERSIFICATION = "vendor_diversification"
    MARKET_EXPANSION = "market_expansion"
    PROCESS_IMPROVEMENT = "process_improvement"
    REVENUE_OPTIMIZATION = "revenue_optimization"

@dataclass
class TrendAnalysis:
    """Analysis of business trends over time."""
    metric_name: str
    current_trend: str  # strong_up, up, stable, down, strong_down
    growth_rate: float
    volatility: float
    seasonality: Optional[Dict[str, float]] = None
    forecast: Optional[Dict[str, float]] = None
    confidence: float = 0.5

@dataclass
class BusinessContext:
    """Comprehensive business context for insights."""
    account_id: str
    industry: Optional[str] = None
    company_size: Optional[str] = None
    business_goals: List[str] = field(default_factory=list)
    risk_tolerance: str = "medium"
    previous_insights: List[Dict] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

class DailySummaryGenerator:
    """
    Advanced engine for generating AI-powered daily business summaries
    with trend analysis, risk assessment, and intelligent recommendations.
    
    Features:
    - Multi-source data aggregation
    - Trend analysis with forecasting
    - Risk identification and prioritization
    - Opportunity detection
    - AI-powered natural language generation
    - Sentiment analysis
    - Confidence scoring
    - Notification triggering
    """
    
    def __init__(self, redis_client, db_pool, kafka_producer, openai_client=None):
        """
        Initialize the daily summary generator.
        
        Args:
            redis_client: Redis client for caching
            db_pool: Database connection pool
            kafka_producer: Kafka producer for events
            openai_client: Optional OpenAI client for AI generation
        """
        self.redis = redis_client
        self.db = db_pool
        self.kafka = kafka_producer
        self.openai = openai_client
        self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize Jinja2 template environment
        self.template_env = Environment(
            loader=FileSystemLoader('templates/insights'),
            autoescape=True
        )
        
        # Templates
        self.templates = self._load_templates()
        
        # Business rules
        self.rules = self._load_business_rules()
        
        self.logger.info("DailySummaryGenerator initialized", 
                        extra={"openai_available": openai_client is not None})

    def _load_templates(self) -> Dict[str, jinja2.Template]:
        """Load all Jinja2 templates."""
        templates = {}
        
        # Executive summary template
        templates['executive'] = jinja2.Template("""
        {% if profit > 0 %}📈{% else %}📉{% endif %} {{ profit_description }}
        {% if events %} | {{ event_summary }}{% endif %}
        {% if high_risks %} | ⚠️ {{ high_risks }} risks{% endif %}
        {% if opportunities %} | 💡 {{ opportunities }} opportunities{% endif %}
        """)
        
        # Detailed summary template
        templates['detailed'] = jinja2.Template("""
        # Executive Summary - {{ date }}
        
        {{ executive_summary }}
        
        ## Key Metrics
        
        {% for name, metric in metrics.items() %}
        ### {{ name }}
        - **Value**: {{ "%.2f"|format(metric.value) }} {{ metric.unit }}
          {% if metric.change %} 
          - **Change**: {{ "%.1f"|format(metric.change_percentage) }}% ({{ metric.trend }})
          - **Previous**: {{ "%.2f"|format(metric.previous_value) }}
          {% endif %}
        {% endfor %}
        
        ## Critical Events ({{ events|length }})
        
        {% for event in events %}
        - **{{ event.type }}**: {{ event.description }}
          - Impact: {{ event.impact }}
          - Time: {{ event.timestamp.strftime('%H:%M') }}
        {% endfor %}
        
        ## Top Recommendations
        
        {% for rec in recommendations[:3] %}
        ### {{ rec.title }}
        {{ rec.description }}
        *Priority: {{ rec.priority }}* | *Expected Impact: {{ rec.expected_impact }}*
        
        **Action Items:**
        {% for item in rec.action_items %}
        - {{ item }}
        {% endfor %}
        {% endfor %}
        
        ## Risk Overview
        
        {% for risk in risks[:5] %}
        - **{{ risk.type }}**: {{ risk.description }}
          - Probability: {{ "%.0f"|format(risk.probability*100) }}%
          - Impact: {{ risk.impact }}
          - Mitigation: {{ risk.mitigation }}
        {% endfor %}
        
        ## Opportunities
        
        {% for opp in opportunities[:3] %}
        - **{{ opp.type }}**: {{ opp.description }}
          - Potential: {{ opp.potential }}
          - Action: {{ opp.action }}
        {% endfor %}
        
        ## Sentiment & Confidence
        
        - **Business Sentiment**: {{ "%.0f"|format(sentiment*100) }}%
        - **Insight Confidence**: {{ "%.0f"|format(confidence*100) }}%
        
        ---
        *Generated at {{ generated_at }}*
        """)
        
        return templates

    def _load_business_rules(self) -> Dict[str, Any]:
        """Load business rules for insights."""
        return {
            'fraud_alert_thresholds': {
                'critical': 3,
                'high': 1
            },
            'profit_margin_target': 0.15,
            'risk_probability_thresholds': {
                'high': 0.7,
                'medium': 0.4,
                'low': 0.1
            },
            'trend_thresholds': {
                'strong_up': 0.1,
                'up': 0.05,
                'down': -0.05,
                'strong_down': -0.1
            }
        }

    @track_generation_time('daily_summary')
    @cache_result(ttl_seconds=3600)  # Cache for 1 hour
    async def generate_daily_summary(
        self, 
        account_id: str,
        target_date: Optional[date] = None,
        business_context: Optional[BusinessContext] = None
    ) -> Optional[DailyInsight]:
        """
        Generate comprehensive daily summary for an account.
        
        Args:
            account_id: Account identifier
            target_date: Date to generate summary for (defaults to yesterday)
            business_context: Optional business context for customization
            
        Returns:
            DailyInsight object if successful
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)
            
        self.logger.info(
            f"Generating daily summary for account: {account_id} on {target_date}",
            extra={"account_id": account_id, "date": target_date.isoformat()}
        )
        
        try:
            # Check if already generated
            existing = await self._check_existing(account_id, target_date)
            if existing:
                self.logger.info(f"Summary already exists for {account_id} on {target_date}")
                return existing
            
            # Get business context if not provided
            if business_context is None:
                business_context = await self._get_business_context(account_id)
            
            # Gather all data in parallel
            data = await self._gather_all_data(account_id, target_date)
            
            # Validate data sufficiency
            if not self._validate_data_sufficiency(data):
                self.logger.warning(f"Insufficient data for account {account_id} on {target_date}")
                raise InsufficientDataError(f"Insufficient data for summary generation")
            
            # Process data
            metrics = data['metrics']
            events = data['events']
            trends = data['trends']
            
            # Generate insights
            risks = await self._identify_risks(account_id, metrics, events, business_context)
            opportunities = await self._identify_opportunities(account_id, metrics, trends, business_context)
            recommendations = await self._generate_recommendations(risks, opportunities, metrics, business_context)
            
            # Generate natural language summaries
            executive_summary = await self._generate_executive_summary(
                metrics, events, risks, opportunities
            )
            
            detailed_summary = self._render_detailed_summary(
                target_date,
                metrics, 
                events, 
                recommendations, 
                risks, 
                opportunities
            )
            
            # Calculate sentiment and confidence
            sentiment = self._calculate_sentiment(events, metrics)
            confidence = self._calculate_confidence(data)
            
            # Create insight
            insight = DailyInsight(
                id=str(uuid.uuid4()),
                account_id=account_id,
                date=target_date,
                summary=detailed_summary,
                executive_summary=executive_summary,
                metrics=metrics,
                key_events=events,
                recommendations=recommendations,
                risks=risks,
                opportunities=opportunities,
                sentiment=sentiment,
                confidence=confidence,
                metadata={
                    'business_context': business_context.__dict__ if business_context else None,
                    'data_points': data.get('data_points', 0),
                    'generation_version': '2.0'
                }
            )
            
            # Store and publish
            await asyncio.gather(
                self._store_insight(insight),
                self._publish_insight(insight),
                self._trigger_notifications(insight),
                self._cache_insight(insight)
            )
            
            self.logger.info(
                f"Daily summary generated for account: {account_id}",
                extra={
                    "account_id": account_id,
                    "date": target_date.isoformat(),
                    "metrics": len(metrics),
                    "events": len(events),
                    "recommendations": len(recommendations),
                    "confidence": confidence
                }
            )
            
            return insight
            
        except InsufficientDataError:
            raise
        except Exception as e:
            self.logger.error(
                f"Failed to generate daily summary: {e}",
                exc_info=True,
                extra={"account_id": account_id, "date": target_date.isoformat()}
            )
            raise GenerationError(f"Daily summary generation failed: {str(e)}")

    async def _gather_all_data(self, account_id: str, target_date: date) -> Dict[str, Any]:
        """Gather all required data in parallel."""
        tasks = [
            self._gather_metrics(account_id, target_date),
            self._gather_events(account_id, target_date),
            self._gather_trends(account_id, target_date),
            self._gather_comparison_data(account_id, target_date),
            self._gather_benchmarks(account_id)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        metrics = results[0] if not isinstance(results[0], Exception) else {}
        events = results[1] if not isinstance(results[1], Exception) else []
        trends = results[2] if not isinstance(results[2], Exception) else {}
        comparisons = results[3] if not isinstance(results[3], Exception) else {}
        benchmarks = results[4] if not isinstance(results[4], Exception) else {}
        
        # Calculate total data points
        data_points = (
            len(metrics) + 
            len(events) + 
            len(trends) + 
            len(comparisons) + 
            len(benchmarks)
        )
        
        return {
            'metrics': metrics,
            'events': events,
            'trends': trends,
            'comparisons': comparisons,
            'benchmarks': benchmarks,
            'data_points': data_points
        }

    async def _gather_metrics(self, account_id: str, target_date: date) -> Dict[str, Metric]:
        """Gather all metrics for the account with enhanced calculations."""
        metrics = {}
        
        async with self.db.acquire() as conn:
            # Current day metrics
            today_stats = await self._get_daily_stats(conn, account_id, target_date)
            
            # Previous day for comparison
            prev_date = target_date - timedelta(days=1)
            prev_stats = await self._get_daily_stats(conn, account_id, prev_date)
            
            # Same day last week for weekly comparison
            week_ago = target_date - timedelta(days=7)
            week_ago_stats = await self._get_daily_stats(conn, account_id, week_ago)
            
            # Month-to-date metrics
            month_start = date(target_date.year, target_date.month, 1)
            mtd_stats = await self._get_period_stats(conn, account_id, month_start, target_date)
            
            # Year-to-date metrics
            year_start = date(target_date.year, 1, 1)
            ytd_stats = await self._get_period_stats(conn, account_id, year_start, target_date)
            
            # Define metrics with comparisons
            metric_definitions = [
                ('transactions', 'Transaction Count', today_stats['transaction_count'], 
                 prev_stats['transaction_count'], week_ago_stats['transaction_count'], 'count'),
                ('revenue', 'Revenue', today_stats['revenue'], 
                 prev_stats['revenue'], week_ago_stats['revenue'], 'currency'),
                ('expenses', 'Expenses', today_stats['expenses'], 
                 prev_stats['expenses'], week_ago_stats['expenses'], 'currency'),
                ('profit', 'Profit', today_stats['profit'], 
                 prev_stats['profit'], week_ago_stats['profit'], 'currency'),
                ('avg_transaction', 'Avg Transaction', today_stats['avg_transaction'], 
                 prev_stats['avg_transaction'], week_ago_stats['avg_transaction'], 'currency'),
                ('unique_vendors', 'Unique Vendors', today_stats['unique_vendors'], 
                 prev_stats['unique_vendors'], week_ago_stats['unique_vendors'], 'count'),
                ('fraud_alerts', 'Fraud Alerts', today_stats['fraud_alerts'], 
                 prev_stats['fraud_alerts'], week_ago_stats['fraud_alerts'], 'count', True),
                ('high_risk_alerts', 'High Risk Alerts', today_stats['high_risk_alerts'], 
                 prev_stats['high_risk_alerts'], week_ago_stats['high_risk_alerts'], 'count', True)
            ]
            
            for key, name, value, prev, week_ago, unit, *warning in metric_definitions:
                if value is not None:
                    metric = self._create_metric(
                        name=name,
                        value=float(value) if value else 0,
                        previous_value=float(prev) if prev else None,
                        week_ago_value=float(week_ago) if week_ago else None,
                        unit=unit,
                        is_warning=bool(warning and warning[0])
                    )
                    metrics[key] = metric
            
            # Add derived metrics
            metrics['profit_margin'] = self._calculate_profit_margin(
                today_stats['profit'], today_stats['revenue']
            )
            
            metrics['runway_days'] = await self._calculate_runway(conn, account_id, target_date)
            
        return metrics

    async def _get_daily_stats(self, conn, account_id: str, target_date: date) -> Dict[str, Any]:
        """Get daily statistics for an account."""
        row = await conn.fetchrow("""
            WITH daily_tx AS (
                SELECT 
                    COUNT(*) as transaction_count,
                    COALESCE(SUM(CASE WHEN type = 'CREDIT' THEN amount ELSE 0 END), 0) as revenue,
                    COALESCE(SUM(CASE WHEN type = 'DEBIT' THEN amount ELSE 0 END), 0) as expenses,
                    COALESCE(AVG(amount), 0) as avg_transaction,
                    COUNT(DISTINCT vendor_id) as unique_vendors
                FROM transactions
                WHERE account_id = $1
                    AND DATE(transaction_date) = $2
                    AND status = 'COMPLETED'
            ),
            daily_alerts AS (
                SELECT 
                    COUNT(*) as fraud_alerts,
                    COUNT(CASE WHEN risk_level = 'HIGH' OR risk_level = 'CRITICAL' THEN 1 END) as high_risk_alerts
                FROM fraud_alerts
                WHERE account_id = $1
                    AND DATE(timestamp) = $2
            )
            SELECT 
                COALESCE(t.transaction_count, 0) as transaction_count,
                COALESCE(t.revenue, 0) as revenue,
                COALESCE(t.expenses, 0) as expenses,
                COALESCE(t.revenue, 0) - COALESCE(t.expenses, 0) as profit,
                COALESCE(t.avg_transaction, 0) as avg_transaction,
                COALESCE(t.unique_vendors, 0) as unique_vendors,
                COALESCE(a.fraud_alerts, 0) as fraud_alerts,
                COALESCE(a.high_risk_alerts, 0) as high_risk_alerts
            FROM daily_tx t
            CROSS JOIN daily_alerts a
        """, account_id, target_date)
        
        return dict(row) if row else {
            'transaction_count': 0,
            'revenue': 0,
            'expenses': 0,
            'profit': 0,
            'avg_transaction': 0,
            'unique_vendors': 0,
            'fraud_alerts': 0,
            'high_risk_alerts': 0
        }

    async def _get_period_stats(self, conn, account_id: str, start_date: date, end_date: date) -> Dict[str, float]:
        """Get statistics for a date period."""
        row = await conn.fetchrow("""
            SELECT 
                COUNT(*) as transaction_count,
                SUM(CASE WHEN type = 'CREDIT' THEN amount ELSE 0 END) as revenue,
                SUM(CASE WHEN type = 'DEBIT' THEN amount ELSE 0 END) as expenses
            FROM transactions
            WHERE account_id = $1
                AND DATE(transaction_date) BETWEEN $2 AND $3
                AND status = 'COMPLETED'
        """, account_id, start_date, end_date)
        
        return dict(row) if row else {
            'transaction_count': 0,
            'revenue': 0,
            'expenses': 0
        }

    def _create_metric(self, name: str, value: float, previous_value: Optional[float] = None,
                      week_ago_value: Optional[float] = None, unit: str = 'count',
                      is_warning: bool = False) -> Metric:
        """Create a metric with comprehensive change calculations."""
        metric = Metric(
            name=name,
            value=value,
            previous_value=previous_value,
            week_ago_value=week_ago_value,
            unit=unit,
            is_warning=is_warning
        )
        
        # Calculate daily change
        if previous_value is not None and previous_value != 0:
            metric.change = value - previous_value
            metric.change_percentage = ((value - previous_value) / abs(previous_value)) * 100
            metric.trend = self._determine_trend(metric.change_percentage)
        
        # Calculate weekly change
        if week_ago_value is not None and week_ago_value != 0:
            metric.weekly_change = value - week_ago_value
            metric.weekly_change_percentage = ((value - week_ago_value) / abs(week_ago_value)) * 100
            metric.weekly_trend = self._determine_trend(metric.weekly_change_percentage)
        
        return metric

    def _determine_trend(self, change_percentage: float) -> str:
        """Determine trend direction from change percentage."""
        thresholds = self.rules['trend_thresholds']
        
        if change_percentage >= thresholds['strong_up'] * 100:
            return 'strong_up'
        elif change_percentage >= thresholds['up'] * 100:
            return 'up'
        elif change_percentage <= thresholds['strong_down'] * 100:
            return 'strong_down'
        elif change_percentage <= thresholds['down'] * 100:
            return 'down'
        else:
            return 'stable'

    def _calculate_profit_margin(self, profit: float, revenue: float) -> Metric:
        """Calculate profit margin metric."""
        margin = (profit / revenue * 100) if revenue > 0 else 0
        return Metric(
            name='Profit Margin',
            value=margin,
            unit='percentage',
            trend='up' if margin > self.rules['profit_margin_target'] * 100 else 'down'
        )

    async def _calculate_runway(self, conn, account_id: str, target_date: date) -> Metric:
        """Calculate cash runway in days."""
        # Get average daily burn over last 30 days
        row = await conn.fetchrow("""
            SELECT 
                AVG(daily_burn) as avg_daily_burn,
                STDDEV(daily_burn) as burn_volatility
            FROM (
                SELECT 
                    DATE(transaction_date) as date,
                    SUM(CASE WHEN type = 'DEBIT' THEN amount ELSE 0 END) * -1 as daily_burn
                FROM transactions
                WHERE account_id = $1
                    AND transaction_date > NOW() - INTERVAL '30 days'
                    AND type = 'DEBIT'
                GROUP BY DATE(transaction_date)
            ) daily
        """, account_id)
        
        avg_daily_burn = float(row['avg_daily_burn']) if row and row['avg_daily_burn'] else 0
        
        # Get current balance
        balance_row = await conn.fetchrow("""
            SELECT COALESCE(SUM(
                CASE 
                    WHEN type = 'CREDIT' THEN amount 
                    WHEN type = 'DEBIT' THEN -amount 
                    ELSE 0 
                END
            ), 0) as balance
            FROM transactions
            WHERE account_id = $1
                AND status = 'COMPLETED'
        """, account_id)
        
        balance = float(balance_row['balance']) if balance_row else 0
        
        if avg_daily_burn > 0:
            runway_days = balance / avg_daily_burn
        else:
            runway_days = float('inf')
        
        return Metric(
            name='Cash Runway',
            value=runway_days,
            unit='days',
            trend='up' if runway_days > 90 else 'stable' if runway_days > 30 else 'down',
            metadata={'balance': balance, 'avg_daily_burn': avg_daily_burn}
        )

    async def _gather_events(self, account_id: str, target_date: date) -> List[KeyEvent]:
        """Gather key events with enhanced categorization."""
        events = []
        
        async with self.db.acquire() as conn:
            # Fraud alerts
            fraud_events = await self._get_fraud_events(conn, account_id, target_date)
            events.extend(fraud_events)
            
            # Large transactions
            large_tx_events = await self._get_large_transaction_events(conn, account_id, target_date)
            events.extend(large_tx_events)
            
            # Anomaly events
            anomaly_events = await self._get_anomaly_events(conn, account_id, target_date)
            events.extend(anomaly_events)
            
            # System events
            system_events = await self._get_system_events(conn, account_id, target_date)
            events.extend(system_events)
            
        return sorted(events, key=lambda x: x.timestamp, reverse=True)

    async def _get_fraud_events(self, conn, account_id: str, target_date: date) -> List[KeyEvent]:
        """Get fraud-related events."""
        rows = await conn.fetch("""
            SELECT * FROM fraud_alerts
            WHERE account_id = $1
                AND DATE(timestamp) = $2
                AND risk_level IN ('HIGH', 'CRITICAL')
            ORDER BY risk_score DESC
            LIMIT 10
        """, account_id, target_date)
        
        events = []
        for row in rows:
            events.append(KeyEvent(
                type='FRAUD_ALERT',
                severity=row['risk_level'],
                description=f"High-risk transaction of ${float(row['amount']):,.2f} detected",
                impact=f"Risk score: {float(row['risk_score']):.2f}",
                timestamp=row['timestamp'],
                related_metrics=['fraud_alerts', 'risk_score'],
                metadata={'alert_id': row['id'], 'transaction_id': row['transaction_id']}
            ))
        
        return events

    async def _get_large_transaction_events(self, conn, account_id: str, target_date: date) -> List[KeyEvent]:
        """Get large transaction events."""
        rows = await conn.fetch("""
            SELECT * FROM transactions
            WHERE account_id = $1
                AND DATE(transaction_date) = $2
                AND amount > 10000
            ORDER BY amount DESC
            LIMIT 5
        """, account_id, target_date)
        
        events = []
        for row in rows:
            events.append(KeyEvent(
                type='LARGE_TRANSACTION',
                severity='INFO',
                description=f"Large {row['type']} of ${float(row['amount']):,.2f}",
                impact=f"Vendor: {row['vendor_name'] or 'Unknown'}",
                timestamp=row['transaction_date'],
                related_metrics=['revenue' if row['type'] == 'CREDIT' else 'expenses'],
                metadata={'transaction_id': row['id'], 'vendor_id': row['vendor_id']}
            ))
        
        return events

    async def _get_anomaly_events(self, conn, account_id: str, target_date: date) -> List[KeyEvent]:
        """Get anomaly detection events."""
        # This would query anomaly detection results
        return []

    async def _get_system_events(self, conn, account_id: str, target_date: date) -> List[KeyEvent]:
        """Get system events (maintenance, updates, etc.)."""
        return []

    async def _gather_trends(self, account_id: str, target_date: date) -> Dict[str, TrendAnalysis]:
        """Analyze trends over time."""
        trends = {}
        
        async with self.db.acquire() as conn:
            # Get last 90 days of data
            rows = await conn.fetch("""
                SELECT 
                    DATE(transaction_date) as date,
                    SUM(CASE WHEN type = 'CREDIT' THEN amount ELSE 0 END) as revenue,
                    SUM(CASE WHEN type = 'DEBIT' THEN amount ELSE 0 END) as expenses,
                    COUNT(*) as transactions
                FROM transactions
                WHERE account_id = $1
                    AND transaction_date > NOW() - INTERVAL '90 days'
                GROUP BY DATE(transaction_date)
                ORDER BY date
            """, account_id)
            
            if len(rows) < 14:  # Need at least 2 weeks for trend analysis
                return trends
            
            df = pd.DataFrame(rows)
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            
            # Analyze each metric
            for col in ['revenue', 'expenses', 'transactions']:
                if col in df.columns:
                    trend = self._analyze_single_trend(df[col].fillna(0))
                    if trend:
                        trends[col] = trend
        
        return trends

    def _analyze_single_trend(self, series: pd.Series) -> Optional[TrendAnalysis]:
        """Analyze trend for a single metric."""
        if len(series) < 14:
            return None
        
        # Calculate moving averages
        ma7 = series.rolling(7).mean()
        ma30 = series.rolling(30).mean()
        
        # Calculate growth rate
        recent_avg = ma7.iloc[-7:].mean()
        previous_avg = ma7.iloc[-14:-7].mean()
        
        if previous_avg > 0:
            growth_rate = (recent_avg - previous_avg) / previous_avg
        else:
            growth_rate = 0
        
        # Determine trend direction
        if growth_rate > self.rules['trend_thresholds']['strong_up']:
            trend_dir = 'strong_up'
        elif growth_rate > self.rules['trend_thresholds']['up']:
            trend_dir = 'up'
        elif growth_rate < self.rules['trend_thresholds']['strong_down']:
            trend_dir = 'strong_down'
        elif growth_rate < self.rules['trend_thresholds']['down']:
            trend_dir = 'down'
        else:
            trend_dir = 'stable'
        
        # Calculate volatility
        volatility = series.pct_change().std()
        
        # Calculate confidence based on data stability
        confidence = max(0, min(1, 1 - volatility))
        
        return TrendAnalysis(
            metric_name=series.name,
            current_trend=trend_dir,
            growth_rate=float(growth_rate),
            volatility=float(volatility),
            confidence=float(confidence)
        )

    async def _gather_comparison_data(self, account_id: str, target_date: date) -> Dict[str, Any]:
        """Gather comparison data against benchmarks."""
        comparisons = {}
        
        async with self.db.acquire() as conn:
            # Compare with industry averages (would need industry data)
            # Compare with historical performance
            pass
        
        return comparisons

    async def _gather_benchmarks(self, account_id: str) -> Dict[str, Any]:
        """Gather benchmark data for the account's industry."""
        benchmarks = {}
        
        # This would query industry benchmark database
        # For now, return empty
        return benchmarks

    async def _get_business_context(self, account_id: str) -> BusinessContext:
        """Get business context for an account."""
        async with self.db.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    industry,
                    company_size,
                    business_goals,
                    risk_tolerance
                FROM accounts
                WHERE account_id = $1
            """, account_id)
            
            if row:
                return BusinessContext(
                    account_id=account_id,
                    industry=row['industry'],
                    company_size=row['company_size'],
                    business_goals=row['business_goals'] or [],
                    risk_tolerance=row['risk_tolerance'] or 'medium'
                )
        
        return BusinessContext(account_id=account_id)

    def _validate_data_sufficiency(self, data: Dict[str, Any]) -> bool:
        """Validate if there's enough data to generate meaningful insights."""
        return data.get('data_points', 0) >= MIN_DATA_POINTS

    async def _identify_risks(
        self, 
        account_id: str, 
        metrics: Dict[str, Metric],
        events: List[KeyEvent],
        context: BusinessContext
    ) -> List[Dict[str, Any]]:
        """Identify and prioritize business risks."""
        risks = []
        
        # Fraud risk
        fraud_alerts = metrics.get('fraud_alerts', Metric(name='', value=0)).value
        if fraud_alerts >= self.rules['fraud_alert_thresholds']['critical']:
            risks.append({
                'type': 'FRAUD',
                'description': f'Critical: {fraud_alerts} fraud alerts detected',
                'probability': 0.8,
                'impact': 'CRITICAL',
                'mitigation': 'Immediately review all flagged transactions',
                'timeframe': 'immediate'
            })
        elif fraud_alerts >= self.rules['fraud_alert_thresholds']['high']:
            risks.append({
                'type': 'FRAUD',
                'description': f'{fraud_alerts} fraud alerts detected',
                'probability': 0.4,
                'impact': 'HIGH',
                'mitigation': 'Review fraud alerts within 24 hours',
                'timeframe': 'today'
            })
        
        # Cash flow risk
        runway = metrics.get('runway_days')
        if runway and runway.value < 30:
            risks.append({
                'type': 'CASH_FLOW',
                'description': f'Critical cash runway: {runway.value:.0f} days',
                'probability': 0.7,
                'impact': 'CRITICAL',
                'mitigation': 'Immediate cash conservation measures needed',
                'timeframe': 'immediate'
            })
        elif runway and runway.value < 90:
            risks.append({
                'type': 'CASH_FLOW',
                'description': f'Limited cash runway: {runway.value:.0f} days',
                'probability': 0.4,
                'impact': 'HIGH',
                'mitigation': 'Review and optimize cash flow',
                'timeframe': 'this_week'
            })
        
        # Profitability risk
        profit_margin = metrics.get('profit_margin')
        if profit_margin and profit_margin.value < 5:  # Less than 5% margin
            risks.append({
                'type': 'PROFITABILITY',
                'description': f'Low profit margin: {profit_margin.value:.1f}%',
                'probability': 0.5,
                'impact': 'HIGH',
                'mitigation': 'Review pricing and cost structure',
                'timeframe': 'this_week'
            })
        
        # Growth risk
        revenue_trend = metrics.get('revenue', Metric(name='', value=0)).trend
        if revenue_trend in ['down', 'strong_down']:
            risks.append({
                'type': 'GROWTH',
                'description': f'Revenue trending {revenue_trend}',
                'probability': 0.4,
                'impact': 'MEDIUM',
                'mitigation': 'Analyze revenue decline drivers',
                'timeframe': 'this_week'
            })
        
        # Adjust probabilities based on risk tolerance
        for risk in risks:
            if context.risk_tolerance == 'high':
                risk['probability'] *= 0.7
            elif context.risk_tolerance == 'low':
                risk['probability'] *= 1.3
        
        return risks

    async def _identify_opportunities(
        self,
        account_id: str,
        metrics: Dict[str, Metric],
        trends: Dict[str, TrendAnalysis],
        context: BusinessContext
    ) -> List[Dict[str, Any]]:
        """Identify business opportunities."""
        opportunities = []
        
        # Growth opportunities
        revenue_trend = trends.get('revenue')
        if revenue_trend and revenue_trend.current_trend in ['strong_up', 'up']:
            opportunities.append({
                'type': OpportunityType.GROWTH.value,
                'description': f'Strong revenue growth trend ({revenue_trend.growth_rate*100:.1f}%)',
                'potential': 'HIGH',
                'action': 'Analyze growth drivers and consider expansion',
                'timeframe': 'next_month'
            })
        
        # Cost optimization opportunities
        expense_trend = trends.get('expenses')
        revenue_growth = revenue_trend.growth_rate if revenue_trend else 0
        expense_growth = expense_trend.growth_rate if expense_trend else 0
        
        if expense_growth > revenue_growth + 0.05:  # Expenses growing faster than revenue
            opportunities.append({
                'type': OpportunityType.COST_OPTIMIZATION.value,
                'description': 'Expenses growing faster than revenue',
                'potential': 'MEDIUM',
                'action': 'Review and optimize expense categories',
                'timeframe': 'this_month'
            })
        
        # Vendor diversification opportunities
        unique_vendors = metrics.get('unique_vendors', Metric(name='', value=0)).value
        if unique_vendors < 5:
            opportunities.append({
                'type': OpportunityType.VENDOR_DIVERSIFICATION.value,
                'description': 'Limited vendor base - diversification opportunity',
                'potential': 'MEDIUM',
                'action': 'Explore additional vendors for key categories',
                'timeframe': 'next_quarter'
            })
        
        # Margin improvement opportunities
        profit_margin = metrics.get('profit_margin')
        if profit_margin and profit_margin.value < 15:
            opportunities.append({
                'type': OpportunityType.REVENUE_OPTIMIZATION.value,
                'description': f'Potential to improve margins from {profit_margin.value:.1f}%',
                'potential': 'MEDIUM',
                'action': 'Review pricing strategy and high-margin products',
                'timeframe': 'this_month'
            })
        
        return opportunities

    async def _generate_recommendations(
        self,
        risks: List[Dict],
        opportunities: List[Dict],
        metrics: Dict[str, Metric],
        context: BusinessContext
    ) -> List[Recommendation]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Convert high-impact risks to recommendations
        for risk in risks:
            if risk.get('impact') in ['CRITICAL', 'HIGH']:
                recommendations.append(Recommendation(
                    title=f"Address {risk['type']} Risk",
                    description=risk['description'],
                    priority=InsightPriority.HIGH if risk['impact'] == 'HIGH' else InsightPriority.CRITICAL,
                    expected_impact=f"Reduce {risk['type'].lower()} exposure",
                    action_items=[risk['mitigation']],
                    timeframe=risk.get('timeframe', 'immediate'),
                    category='risk_mitigation'
                ))
        
        # Convert high-potential opportunities to recommendations
        for opp in opportunities:
            if opp.get('potential') == 'HIGH':
                recommendations.append(Recommendation(
                    title=f"Capitalize on {opp['type']} Opportunity",
                    description=opp['description'],
                    priority=InsightPriority.MEDIUM,
                    expected_impact=opp['potential'],
                    action_items=[opp['action']],
                    timeframe=opp.get('timeframe', 'this_month'),
                    category='opportunity'
                ))
        
        # Add specific metric-based recommendations
        profit_margin = metrics.get('profit_margin')
        if profit_margin and profit_margin.value < 10:
            recommendations.append(Recommendation(
                title="Improve Profit Margins",
                description=f"Current profit margin is {profit_margin.value:.1f}% - below target",
                priority=InsightPriority.MEDIUM,
                expected_impact="Increase profitability",
                action_items=[
                    "Review pricing strategy",
                    "Identify cost reduction opportunities",
                    "Analyze high-margin product lines"
                ],
                timeframe='this_month',
                category='performance_improvement'
            ))
        
        # Add trend-based recommendations
        revenue_trend = metrics.get('revenue', Metric(name='', value=0)).trend
        if revenue_trend in ['down', 'strong_down']:
            recommendations.append(Recommendation(
                title="Address Revenue Decline",
                description=f"Revenue is trending {revenue_trend}",
                priority=InsightPriority.HIGH,
                expected_impact="Reverse revenue decline",
                action_items=[
                    "Analyze customer churn",
                    "Review sales pipeline",
                    "Consider promotional activities"
                ],
                timeframe='this_week',
                category='revenue_growth'
            ))
        
        return recommendations

    async def _generate_executive_summary(
        self,
        metrics: Dict[str, Metric],
        events: List[KeyEvent],
        risks: List[Dict],
        opportunities: List[Dict]
    ) -> str:
        """Generate concise executive summary."""
        
        # Use OpenAI if available for more natural language
        if self.openai:
            try:
                return await self._generate_ai_summary(metrics, events, risks, opportunities)
            except Exception as e:
                self.logger.error(f"OpenAI generation failed: {e}")
        
        # Fallback to template-based summary
        return self._render_executive_summary(metrics, events, risks, opportunities)

    async def _generate_ai_summary(
        self,
        metrics: Dict[str, Metric],
        events: List[KeyEvent],
        risks: List[Dict],
        opportunities: List[Dict]
    ) -> str:
        """Generate summary using OpenAI."""
        
        profit = metrics.get('profit', Metric(name='', value=0))
        revenue = metrics.get('revenue', Metric(name='', value=0))
        
        prompt = f"""
        Generate a concise executive summary for a CEO based on this daily business data:
        
        Key Metrics:
        - Profit: ${profit.value:,.2f} ({profit.trend} by {abs(profit.change_percentage or 0):.1f}%)
        - Revenue: ${revenue.value:,.2f}
        - Transactions: {metrics.get('transactions', Metric(name='', value=0)).value}
        - Fraud Alerts: {metrics.get('fraud_alerts', Metric(name='', value=0)).value}
        
        Events: {len(events)} key events
        High-priority risks: {len([r for r in risks if r.get('impact') in ['CRITICAL', 'HIGH']])}
        Opportunities: {len(opportunities)}
        
        Keep it under 100 words, focus on what matters most for a CEO's daily briefing.
        Use a professional but engaging tone. Start with the most important item.
        """
        
        try:
            response = await self.openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are an executive business analyst creating daily briefings for CEOs."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            self.logger.error(f"OpenAI API error: {e}")
            raise

    def _render_executive_summary(
        self,
        metrics: Dict[str, Metric],
        events: List[KeyEvent],
        risks: List[Dict],
        opportunities: List[Dict]
    ) -> str:
        """Render executive summary using template."""
        
        profit = metrics.get('profit', Metric(name='', value=0))
        
        profit_desc = f"Profit of ${profit.value:,.2f}"
        if profit.trend in ['up', 'strong_up']:
            profit_desc += f" (↑ {abs(profit.change_percentage or 0):.1f}%)"
        elif profit.trend in ['down', 'strong_down']:
            profit_desc += f" (↓ {abs(profit.change_percentage or 0):.1f}%)"
        
        high_risks = len([r for r in risks if r.get('impact') in ['CRITICAL', 'HIGH']])
        high_opps = len([o for o in opportunities if o.get('potential') == 'HIGH'])
        
        event_summary = f"{len(events)} events" if events else ""
        
        return self.templates['executive'].render(
            profit=profit.value,
            profit_description=profit_desc,
            event_summary=event_summary,
            events=events,
            high_risks=high_risks,
            opportunities=high_opps
        ).strip()

    def _render_detailed_summary(
        self,
        target_date: date,
        metrics: Dict[str, Metric],
        events: List[KeyEvent],
        recommendations: List[Recommendation],
        risks: List[Dict],
        opportunities: List[Dict]
    ) -> str:
        """Render detailed summary using template."""
        
        return self.templates['detailed'].render(
            date=target_date.strftime('%B %d, %Y'),
            executive_summary="Daily Business Summary",
            metrics=metrics,
            events=events,
            recommendations=recommendations,
            risks=risks,
            opportunities=opportunities,
            sentiment=self._calculate_sentiment(events, metrics),
            confidence=self._calculate_confidence({'data_points': len(metrics) + len(events)}),
            generated_at=datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
        )

    def _calculate_sentiment(self, events: List[KeyEvent], metrics: Dict[str, Metric]) -> float:
        """Calculate overall business sentiment."""
        sentiment = 0.5  # Neutral baseline
        
        # Adjust based on events
        for event in events:
            if event.type == 'FRAUD_ALERT':
                sentiment -= SENTIMENT_WEIGHT_EVENT
            elif event.type == 'LARGE_TRANSACTION':
                if 'CREDIT' in event.description:
                    sentiment += SENTIMENT_WEIGHT_EVENT * 0.5
                else:
                    sentiment -= SENTIMENT_WEIGHT_EVENT * 0.5
        
        # Adjust based on metrics
        profit = metrics.get('profit')
        if profit and profit.change_percentage:
            sentiment += (profit.change_percentage / 100) * SENTIMENT_WEIGHT_METRIC
        
        # Adjust based on trends
        revenue = metrics.get('revenue')
        if revenue and revenue.trend in ['strong_up', 'up']:
            sentiment += 0.05
        elif revenue and revenue.trend in ['strong_down', 'down']:
            sentiment -= 0.05
        
        return max(0, min(1, sentiment))

    def _calculate_confidence(self, data: Dict[str, Any]) -> float:
        """Calculate confidence in the insights based on data quality."""
        data_points = data.get('data_points', 0)
        
        if data_points > HIGH_CONFIDENCE_THRESHOLD:
            return 0.95
        elif data_points > MEDIUM_CONFIDENCE_THRESHOLD:
            return 0.85
        elif data_points > LOW_CONFIDENCE_THRESHOLD:
            return 0.70
        else:
            return 0.50

    async def _check_existing(self, account_id: str, target_date: date) -> Optional[DailyInsight]:
        """Check if summary already exists for this date."""
        async with self.db.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM daily_insights
                WHERE account_id = $1 AND date = $2
            """, account_id, target_date)
            
            if row:
                return DailyInsight(**dict(row))
            return None

    async def _store_insight(self, insight: DailyInsight):
        """Store insight in database."""
        async with self.db.acquire() as conn:
            await conn.execute("""
                INSERT INTO daily_insights (
                    id, account_id, date, summary, executive_summary,
                    metrics, key_events, recommendations, risks, opportunities,
                    sentiment, confidence, metadata, generated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (account_id, date) DO UPDATE SET
                    summary = EXCLUDED.summary,
                    executive_summary = EXCLUDED.executive_summary,
                    metrics = EXCLUDED.metrics,
                    key_events = EXCLUDED.key_events,
                    recommendations = EXCLUDED.recommendations,
                    risks = EXCLUDED.risks,
                    opportunities = EXCLUDED.opportunities,
                    sentiment = EXCLUDED.sentiment,
                    confidence = EXCLUDED.confidence,
                    metadata = EXCLUDED.metadata,
                    generated_at = EXCLUDED.generated_at
            """,
                insight.id,
                insight.account_id,
                insight.date,
                insight.summary,
                insight.executive_summary,
                json.dumps({k: v.dict() for k, v in insight.metrics.items()}),
                json.dumps([e.dict() for e in insight.key_events]),
                json.dumps([r.dict() for r in insight.recommendations]),
                json.dumps(insight.risks),
                json.dumps(insight.opportunities),
                insight.sentiment,
                insight.confidence,
                json.dumps(insight.metadata) if insight.metadata else None,
                insight.generated_at
            )

    async def _publish_insight(self, insight: DailyInsight):
        """Publish insight to Kafka."""
        try:
            await self.kafka.send(
                'daily_insights',
                key=insight.account_id,
                value=insight.model_dump_json()
            )
            self.logger.debug(f"Insight published for account {insight.account_id}")
        except Exception as e:
            self.logger.error(f"Failed to publish insight: {e}")

    async def _cache_insight(self, insight: DailyInsight):
        """Cache insight in Redis."""
        try:
            cache_key = f"insight:{insight.account_id}:{insight.date}"
            await self.redis.setex(
                cache_key,
                CACHE_TTL_HOURS * 3600,
                insight.model_dump_json()
            )
        except Exception as e:
            self.logger.error(f"Failed to cache insight: {e}")

    async def _trigger_notifications(self, insight: DailyInsight):
        """Trigger notifications for high-priority items."""
        high_priority_recs = [
            r for r in insight.recommendations 
            if r.priority in [InsightPriority.CRITICAL, InsightPriority.HIGH]
        ]
        
        critical_risks = [
            r for r in insight.risks 
            if r.get('impact') == 'CRITICAL'
        ]
        
        if high_priority_recs or critical_risks:
            try:
                await self.kafka.send(
                    'notifications',
                    key=insight.account_id,
                    value=json.dumps({
                        'type': 'daily_insight_high_priority',
                        'account_id': insight.account_id,
                        'recommendations': [r.dict() for r in high_priority_recs],
                        'critical_risks': critical_risks,
                        'summary': insight.executive_summary,
                        'timestamp': datetime.utcnow().isoformat()
                    })
                )
                self.logger.info(
                    f"High-priority notifications triggered for account {insight.account_id}",
                    extra={
                        "account_id": insight.account_id,
                        "recommendations": len(high_priority_recs),
                        "critical_risks": len(critical_risks)
                    }
                )
            except Exception as e:
                self.logger.error(f"Failed to trigger notifications: {e}")

    async def health_check(self) -> Dict[str, Any]:
        """Health check endpoint."""
        return {
            'status': 'healthy',
            'service': 'daily_summary_generator',
            'templates_loaded': len(self.templates),
            'openai_available': self.openai is not None,
            'timestamp': datetime.utcnow().isoformat()
        }