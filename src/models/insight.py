"""
Production-grade Pydantic models for business insights and recommendations.
Provides comprehensive data structures for daily summaries, metrics, events,
and actionable recommendations.
"""

from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from datetime import datetime, date, timezone
from typing import List, Optional, Dict, Any, Union
from enum import Enum
import uuid
import json
from decimal import Decimal

# ============================================================================
# Enums
# ============================================================================

class InsightType(str, Enum):
    DAILY_SUMMARY = "DAILY_SUMMARY"
    WEEKLY_REVIEW = "WEEKLY_REVIEW"
    MONTHLY_REPORT = "MONTHLY_REPORT"
    TREND_ALERT = "TREND_ALERT"
    OPPORTUNITY = "OPPORTUNITY"
    RISK_WARNING = "RISK_WARNING"
    PERFORMANCE_REVIEW = "PERFORMANCE_REVIEW"
    ANOMALY_DETECTION = "ANOMALY_DETECTION"
    FORECAST_UPDATE = "FORECAST_UPDATE"
    
    @property
    def description(self) -> str:
        return {
            InsightType.DAILY_SUMMARY: "Daily business summary",
            InsightType.WEEKLY_REVIEW: "Weekly performance review",
            InsightType.MONTHLY_REPORT: "Monthly business report",
            InsightType.TREND_ALERT: "Significant trend detected",
            InsightType.OPPORTUNITY: "Business opportunity identified",
            InsightType.RISK_WARNING: "Risk warning",
            InsightType.PERFORMANCE_REVIEW: "Performance review",
            InsightType.ANOMALY_DETECTION: "Anomaly detected",
            InsightType.FORECAST_UPDATE: "Forecast update",
        }[self]

class InsightPriority(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    
    @property
    def numeric_value(self) -> int:
        return {
            InsightPriority.CRITICAL: 4,
            InsightPriority.HIGH: 3,
            InsightPriority.MEDIUM: 2,
            InsightPriority.LOW: 1,
        }[self]
    
    @property
    def color(self) -> str:
        return {
            InsightPriority.CRITICAL: "#dc3545",
            InsightPriority.HIGH: "#fd7e14",
            InsightPriority.MEDIUM: "#ffc107",
            InsightPriority.LOW: "#28a745",
        }[self]

class InsightStatus(str, Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"
    ARCHIVED = "ARCHIVED"
    DISMISSED = "DISMISSED"
    
    @property
    def is_active(self) -> bool:
        return self in [InsightStatus.ACTIVE, InsightStatus.ACKNOWLEDGED]
    
    @property
    def is_resolved(self) -> bool:
        return self in [InsightStatus.RESOLVED, InsightStatus.ARCHIVED]

class TrendDirection(str, Enum):
    STRONG_UP = "strong_up"
    UP = "up"
    STABLE = "stable"
    DOWN = "down"
    STRONG_DOWN = "strong_down"
    
    @property
    def emoji(self) -> str:
        return {
            TrendDirection.STRONG_UP: "📈",
            TrendDirection.UP: "⬆️",
            TrendDirection.STABLE: "➡️",
            TrendDirection.DOWN: "⬇️",
            TrendDirection.STRONG_DOWN: "📉",
        }[self]
    
    @property
    def is_positive(self) -> bool:
        return self in [TrendDirection.UP, TrendDirection.STRONG_UP]

class MetricCategory(str, Enum):
    REVENUE = "revenue"
    EXPENSES = "expenses"
    PROFITABILITY = "profitability"
    FRAUD = "fraud"
    GROWTH = "growth"
    CUSTOMER = "customer"
    OPERATIONAL = "operational"
    RISK = "risk"

class EventSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    
    @property
    def color(self) -> str:
        return {
            EventSeverity.INFO: "#17a2b8",
            EventSeverity.WARNING: "#ffc107",
            EventSeverity.ERROR: "#dc3545",
            EventSeverity.CRITICAL: "#721c24",
        }[self]

# ============================================================================
# Models
# ============================================================================

class Metric(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    name: str = Field(..., min_length=1, max_length=100)
    value: float = Field(..., ge=0)
    
    previous_value: Optional[float] = None
    week_ago_value: Optional[float] = None
    month_ago_value: Optional[float] = None
    
    change: Optional[float] = None
    change_percentage: Optional[float] = Field(None, ge=-100, le=1000)
    weekly_change: Optional[float] = None
    weekly_change_percentage: Optional[float] = None
    
    trend: Optional[TrendDirection] = None
    weekly_trend: Optional[TrendDirection] = None
    
    category: Optional[MetricCategory] = None
    unit: str = Field(default="count", pattern="^(count|currency|percentage|days|rate|score)$")
    benchmark: Optional[float] = None
    target: Optional[float] = None
    is_warning: bool = False
    confidence: float = Field(default=1.0, ge=0, le=1)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator('change_percentage', 'weekly_change_percentage')
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        return round(v, 2) if v is not None else v
    
    @field_validator('change', 'weekly_change')
    @classmethod
    def validate_change(cls, v: Optional[float]) -> Optional[float]:
        return round(v, 2) if v is not None else v
    
    @model_validator(mode='after')
    def calculate_changes(self):
        if self.previous_value is not None and self.change is None:
            self.change = self.value - self.previous_value
            
        if (self.previous_value is not None and 
            self.change_percentage is None and 
            self.previous_value != 0):
            self.change_percentage = (self.change / abs(self.previous_value)) * 100
            
        return self
    
    def get_formatted_value(self) -> str:
        if self.unit == 'currency':
            return f"${self.value:,.2f}"
        elif self.unit == 'percentage':
            return f"{self.value:.1f}%"
        elif self.unit == 'days':
            return f"{self.value:.0f} days"
        else:
            return f"{self.value:,.0f}"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'value': self.value,
            'formatted_value': self.get_formatted_value(),
            'change': self.change,
            'change_percentage': self.change_percentage,
            'trend': self.trend.value if self.trend else None,
            'trend_emoji': self.trend.emoji if self.trend else None,
            'category': self.category.value if self.category else None,
            'unit': self.unit,
            'is_warning': self.is_warning
        }

class KeyEvent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: str = Field(..., min_length=1, max_length=50)
    severity: EventSeverity = Field(default=EventSeverity.INFO)
    description: str = Field(..., min_length=1, max_length=500)
    impact: str = Field(..., max_length=200)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    related_metrics: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'type': self.type,
            'severity': self.severity.value,
            'description': self.description,
            'impact': self.impact,
            'timestamp': self.timestamp.isoformat(),
            'time_ago': self.get_time_ago()
        }
    
    def get_time_ago(self) -> str:
        delta = datetime.now(timezone.utc) - self.timestamp
        if delta.days > 0:
            return f"{delta.days}d ago"
        elif delta.seconds > 3600:
            return f"{delta.seconds // 3600}h ago"
        elif delta.seconds > 60:
            return f"{delta.seconds // 60}m ago"
        else:
            return "just now"

class Recommendation(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str = Field(..., min_length=1, max_length=200)
    description: str = Field(..., max_length=1000)
    priority: InsightPriority
    expected_impact: str = Field(..., max_length=200)
    action_items: List[str] = Field(..., min_length=1)
    category: str = Field(default="general")
    timeframe: str = Field(default="immediate", pattern="^(immediate|today|this_week|this_month|next_quarter)$")
    metrics_affected: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'priority': self.priority.value,
            'priority_color': self.priority.color,
            'expected_impact': self.expected_impact,
            'action_items': self.action_items,
            'timeframe': self.timeframe
        }
    
    @property
    def is_expired(self) -> bool:
        if self.expires_at:
            return datetime.now(timezone.utc) > self.expires_at
        return False

class DailyInsight(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    account_id: str = Field(..., min_length=1, max_length=100)
    date: date
    
    summary: str = Field(..., max_length=10000)
    executive_summary: str = Field(..., max_length=500)
    
    metrics: Dict[str, Metric] = Field(default_factory=dict)
    key_events: List[KeyEvent] = Field(default_factory=list)
    recommendations: List[Recommendation] = Field(default_factory=list)
    
    risks: List[Dict[str, Any]] = Field(default_factory=list)
    opportunities: List[Dict[str, Any]] = Field(default_factory=list)
    
    sentiment: float = Field(..., ge=-1, le=1)
    confidence: float = Field(..., ge=0, le=1)
    
    type: InsightType = Field(default=InsightType.DAILY_SUMMARY)
    status: InsightStatus = Field(default=InsightStatus.ACTIVE)
    
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    version: str = Field(default="2.0")
    
    @field_validator('sentiment')
    @classmethod
    def validate_sentiment(cls, v: float) -> float:
        return round(v, 2)
    
    @field_validator('confidence')
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        return round(v, 2)
    
    @model_validator(mode='after')
    def set_period_dates(self):
        if not self.period_start:
            self.period_start = datetime.combine(self.date, datetime.min.time(), tzinfo=timezone.utc)
        if not self.period_end:
            self.period_end = datetime.combine(self.date, datetime.max.time(), tzinfo=timezone.utc)
        return self
    
    def get_metrics_by_category(self, category: MetricCategory) -> List[Metric]:
        return [m for m in self.metrics.values() if m.category == category]
    
    def get_high_priority_recommendations(self) -> List[Recommendation]:
        return [r for r in self.recommendations if r.priority in [InsightPriority.CRITICAL, InsightPriority.HIGH]]
    
    def get_critical_events(self) -> List[KeyEvent]:
        return [e for e in self.key_events if e.severity in [EventSeverity.CRITICAL, EventSeverity.ERROR]]
    
    def get_risk_summary(self) -> Dict[str, Any]:
        risk_summary = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0, 'total': len(self.risks)}
        for risk in self.risks:
            impact = risk.get('impact', 'LOW').upper()
            if impact in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
                risk_summary[impact.lower()] += 1
        return risk_summary
    
    def get_sentiment_label(self) -> str:
        if self.sentiment >= 0.5:
            return "Very Positive"
        elif self.sentiment >= 0.2:
            return "Positive"
        elif self.sentiment >= -0.2:
            return "Neutral"
        elif self.sentiment >= -0.5:
            return "Negative"
        else:
            return "Very Negative"
    
    def to_summary_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'account_id': self.account_id,
            'date': self.date.isoformat(),
            'executive_summary': self.executive_summary,
            'metrics': {name: metric.to_dict() for name, metric in self.metrics.items()},
            'key_events': [e.to_dict() for e in self.key_events[:5]],
            'recommendations': [r.to_dict() for r in self.get_high_priority_recommendations()],
            'risk_summary': self.get_risk_summary(),
            'sentiment': self.sentiment,
            'sentiment_label': self.get_sentiment_label(),
            'confidence': self.confidence,
            'generated_at': self.generated_at.isoformat()
        }
    
    def to_kafka_message(self) -> bytes:
        return json.dumps(self.model_dump(mode='json'), default=str).encode('utf-8')
    
    @classmethod
    def from_kafka_message(cls, data: bytes):
        return cls(**json.loads(data.decode('utf-8')))

# ============================================================================
# Additional Models for Specific Use Cases
# ============================================================================

class TrendAlert(BaseModel):
    metric_name: str
    trend: TrendDirection
    change_percentage: float
    duration_days: int
    severity: EventSeverity
    description: str
    recommendation: Optional[str] = None

class PerformanceReview(BaseModel):
    period_start: date
    period_end: date
    metrics_summary: Dict[str, Dict[str, float]]
    top_improvements: List[str]
    top_declines: List[str]
    highlights: List[str]
    lowlights: List[str]
    overall_score: float
    recommendations: List[Recommendation]

class Forecast(BaseModel):
    metric_name: str
    forecast_values: List[float]
    confidence_intervals: Dict[str, List[float]]
    methodology: str
    factors: List[str]
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# ============================================================================
# Export
# ============================================================================

__all__ = [
    'InsightType',
    'InsightPriority',
    'InsightStatus',
    'TrendDirection',
    'MetricCategory',
    'EventSeverity',
    'Metric',
    'KeyEvent',
    'Recommendation',
    'DailyInsight',
    'TrendAlert',
    'PerformanceReview',
    'Forecast',
]