-- =====================================================
-- CEO OS Insight Engine Database Schema
-- Version: 2.0
-- Description: Production-grade schema for business insights,
--              metrics, and scheduled jobs
-- =====================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- =====================================================
-- ENUM TYPES
-- =====================================================

-- Insight type enum
DO $$ BEGIN
    CREATE TYPE insight_type AS ENUM (
        'DAILY_SUMMARY',
        'WEEKLY_REVIEW',
        'MONTHLY_REPORT',
        'TREND_ALERT',
        'OPPORTUNITY',
        'RISK_WARNING',
        'PERFORMANCE_REVIEW',
        'ANOMALY_DETECTION',
        'FORECAST_UPDATE'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Insight priority enum
DO $$ BEGIN
    CREATE TYPE insight_priority AS ENUM (
        'CRITICAL',
        'HIGH',
        'MEDIUM',
        'LOW'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Insight status enum
DO $$ BEGIN
    CREATE TYPE insight_status AS ENUM (
        'PENDING',
        'ACTIVE',
        'ACKNOWLEDGED',
        'RESOLVED',
        'ARCHIVED',
        'DISMISSED'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Metric category enum
DO $$ BEGIN
    CREATE TYPE metric_category AS ENUM (
        'revenue',
        'expenses',
        'profitability',
        'fraud',
        'growth',
        'customer',
        'operational',
        'risk'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Job status enum
DO $$ BEGIN
    CREATE TYPE job_status AS ENUM (
        'PENDING',
        'RUNNING',
        'COMPLETED',
        'FAILED',
        'RETRYING',
        'CANCELLED'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Job type enum
DO $$ BEGIN
    CREATE TYPE job_type AS ENUM (
        'DAILY_INSIGHT_GENERATION',
        'WEEKLY_REVIEW_GENERATION',
        'MONTHLY_REPORT_GENERATION',
        'TREND_ANALYSIS',
        'DATA_CLEANUP',
        'CACHE_WARMUP',
        'MODEL_RETRAINING'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- =====================================================
-- DAILY INSIGHTS TABLE
-- =====================================================

CREATE TABLE IF NOT EXISTS daily_insights (
    -- Primary identifiers
    id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR(36),
    account_id VARCHAR(100) NOT NULL,
    
    -- Insight metadata
    type insight_type NOT NULL DEFAULT 'DAILY_SUMMARY',
    priority insight_priority NOT NULL DEFAULT 'MEDIUM',
    status insight_status NOT NULL DEFAULT 'ACTIVE',
    
    -- Date information
    date DATE NOT NULL,
    period_start TIMESTAMP WITH TIME ZONE,
    period_end TIMESTAMP WITH TIME ZONE,
    
    -- Summaries
    summary TEXT,
    executive_summary TEXT,
    
    -- Data components (JSONB for flexibility)
    metrics JSONB NOT NULL DEFAULT '{}'::JSONB,
    key_events JSONB NOT NULL DEFAULT '[]'::JSONB,
    recommendations JSONB NOT NULL DEFAULT '[]'::JSONB,
    risks JSONB NOT NULL DEFAULT '[]'::JSONB,
    opportunities JSONB NOT NULL DEFAULT '[]'::JSONB,
    
    -- Quality metrics
    sentiment DECIMAL(4,3) CHECK (sentiment >= -1 AND sentiment <= 1),
    confidence DECIMAL(4,3) CHECK (confidence >= 0 AND confidence <= 1),
    
    -- Categorization
    tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    
    -- Version and metadata
    version VARCHAR(10) DEFAULT '2.0',
    metadata JSONB DEFAULT '{}'::JSONB,
    
    -- Audit trail
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    generated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Soft delete
    deleted_at TIMESTAMP WITH TIME ZONE,
    
    -- Constraints
    CONSTRAINT valid_dates CHECK (period_start <= period_end),
    CONSTRAINT valid_account_id CHECK (account_id ~ '^[A-Za-z0-9_-]+$'),
    UNIQUE(account_id, date)
);

-- =====================================================
-- RECOMMENDATIONS TABLE (for tracking actions)
-- =====================================================

CREATE TABLE IF NOT EXISTS recommendations (
    id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR(36),
    insight_id VARCHAR(36) REFERENCES daily_insights(id) ON DELETE CASCADE,
    account_id VARCHAR(100) NOT NULL,
    
    -- Recommendation details
    title VARCHAR(200) NOT NULL,
    description TEXT,
    priority insight_priority NOT NULL,
    expected_impact VARCHAR(200),
    action_items JSONB NOT NULL DEFAULT '[]'::JSONB,
    category VARCHAR(50) DEFAULT 'general',
    timeframe VARCHAR(20) DEFAULT 'immediate',
    
    -- Metrics affected
    metrics_affected TEXT[] DEFAULT ARRAY[]::TEXT[],
    
    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::JSONB
);

-- =====================================================
-- METRICS HISTORY TABLE (for time series analysis)
-- =====================================================

CREATE TABLE IF NOT EXISTS metrics_history (
    id BIGSERIAL PRIMARY KEY,
    account_id VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_category metric_category,
    value DECIMAL(19,4) NOT NULL,
    date DATE NOT NULL,
    
    -- Additional context
    metadata JSONB DEFAULT '{}'::JSONB,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(account_id, metric_name, date)
);

-- =====================================================
-- SCHEDULED JOBS TABLE
-- =====================================================

CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type job_type NOT NULL,
    account_id VARCHAR(100),
    
    -- Scheduling
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Execution
    status job_status NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    
    -- Results
    result JSONB,
    error TEXT,
    error_details JSONB,
    
    -- Performance
    duration_ms INTEGER,
    priority INTEGER DEFAULT 5, -- 1-10, 1 highest
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::JSONB,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX (job_type, status),
    INDEX (scheduled_at),
    INDEX (account_id)
);

-- =====================================================
-- INSIGHT FEEDBACK TABLE
-- =====================================================

CREATE TABLE IF NOT EXISTS insight_feedback (
    id BIGSERIAL PRIMARY KEY,
    insight_id VARCHAR(36) REFERENCES daily_insights(id) ON DELETE CASCADE,
    account_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(100),
    
    -- Feedback
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    helpful BOOLEAN,
    
    -- Specific feedback on components
    useful_metrics TEXT[],
    useful_recommendations TEXT[],
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::JSONB,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(insight_id, user_id)
);

-- =====================================================
-- INDEXES
-- =====================================================

-- Daily insights indexes
CREATE INDEX IF NOT EXISTS idx_insights_account ON daily_insights(account_id);
CREATE INDEX IF NOT EXISTS idx_insights_date ON daily_insights(date DESC);
CREATE INDEX IF NOT EXISTS idx_insights_account_date ON daily_insights(account_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_insights_type ON daily_insights(type);
CREATE INDEX IF NOT EXISTS idx_insights_priority ON daily_insights(priority);
CREATE INDEX IF NOT EXISTS idx_insights_status ON daily_insights(status);
CREATE INDEX IF NOT EXISTS idx_insights_sentiment ON daily_insights(sentiment);
CREATE INDEX IF NOT EXISTS idx_insights_confidence ON daily_insights(confidence);
CREATE INDEX IF NOT EXISTS idx_insights_created ON daily_insights(created_at DESC);

-- GIN indexes for JSONB queries
CREATE INDEX IF NOT EXISTS idx_insights_metrics ON daily_insights USING GIN (metrics);
CREATE INDEX IF NOT EXISTS idx_insights_events ON daily_insights USING GIN (key_events);
CREATE INDEX IF NOT EXISTS idx_insights_recs ON daily_insights USING GIN (recommendations);
CREATE INDEX IF NOT EXISTS idx_insights_tags ON daily_insights USING GIN (tags);
CREATE INDEX IF NOT EXISTS idx_insights_metadata ON daily_insights USING GIN (metadata);

-- Recommendations indexes
CREATE INDEX IF NOT EXISTS idx_recommendations_insight ON recommendations(insight_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_account ON recommendations(account_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_priority ON recommendations(priority);
CREATE INDEX IF NOT EXISTS idx_recommendations_status ON recommendations(status);

-- Metrics history indexes
CREATE INDEX IF NOT EXISTS idx_metrics_account_date ON metrics_history(account_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_name_date ON metrics_history(metric_name, date DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_category ON metrics_history(metric_category);

-- Feedback indexes
CREATE INDEX IF NOT EXISTS idx_feedback_insight ON insight_feedback(insight_id);
CREATE INDEX IF NOT EXISTS idx_feedback_account ON insight_feedback(account_id);
CREATE INDEX IF NOT EXISTS idx_feedback_rating ON insight_feedback(rating);

-- =====================================================
-- TRIGGERS AND FUNCTIONS
-- =====================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for daily_insights
DROP TRIGGER IF EXISTS update_daily_insights_updated_at ON daily_insights;
CREATE TRIGGER update_daily_insights_updated_at
    BEFORE UPDATE ON daily_insights
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for recommendations
DROP TRIGGER IF EXISTS update_recommendations_updated_at ON recommendations;
CREATE TRIGGER update_recommendations_updated_at
    BEFORE UPDATE ON recommendations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for scheduled_jobs
DROP TRIGGER IF EXISTS update_scheduled_jobs_updated_at ON scheduled_jobs;
CREATE TRIGGER update_scheduled_jobs_updated_at
    BEFORE UPDATE ON scheduled_jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Function to automatically update period dates based on insight date
CREATE OR REPLACE FUNCTION set_insight_period_dates()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.period_start IS NULL THEN
        NEW.period_start = (NEW.date || ' 00:00:00')::TIMESTAMP WITH TIME ZONE;
    END IF;
    IF NEW.period_end IS NULL THEN
        NEW.period_end = (NEW.date || ' 23:59:59')::TIMESTAMP WITH TIME ZONE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for setting period dates
DROP TRIGGER IF EXISTS set_insight_period_dates ON daily_insights;
CREATE TRIGGER set_insight_period_dates
    BEFORE INSERT ON daily_insights
    FOR EACH ROW
    EXECUTE FUNCTION set_insight_period_dates();

-- =====================================================
-- VIEWS
-- =====================================================

-- Account summary view
CREATE OR REPLACE VIEW account_insight_summary AS
SELECT 
    account_id,
    COUNT(*) as total_insights,
    COUNT(CASE WHEN priority = 'CRITICAL' THEN 1 END) as critical_insights,
    COUNT(CASE WHEN priority = 'HIGH' THEN 1 END) as high_insights,
    AVG(confidence) as avg_confidence,
    AVG(sentiment) as avg_sentiment,
    MAX(date) as latest_insight_date,
    MIN(date) as first_insight_date
FROM daily_insights
WHERE deleted_at IS NULL
GROUP BY account_id;

-- Insight quality view
CREATE OR REPLACE VIEW insight_quality_metrics AS
SELECT 
    DATE_TRUNC('month', date) as month,
    COUNT(*) as total_generated,
    AVG(confidence) as avg_confidence,
    AVG(sentiment) as avg_sentiment,
    COUNT(CASE WHEN confidence >= 0.9 THEN 1 END)::FLOAT / COUNT(*) as high_confidence_ratio,
    COUNT(CASE WHEN sentiment >= 0.5 THEN 1 END)::FLOAT / COUNT(*) as positive_sentiment_ratio
FROM daily_insights
GROUP BY DATE_TRUNC('month', date)
ORDER BY month DESC;

-- Recent recommendations view
CREATE OR REPLACE VIEW recent_recommendations AS
SELECT 
    r.*,
    i.account_id,
    i.date as insight_date,
    i.executive_summary as insight_summary
FROM recommendations r
JOIN daily_insights i ON r.insight_id = i.id
WHERE i.deleted_at IS NULL
ORDER BY r.created_at DESC
LIMIT 100;

-- =====================================================
-- PARTITIONING (for very large tables)
-- =====================================================

-- Note: Uncomment for production with large data volumes
/*
-- Create partitioned metrics_history table
CREATE TABLE IF NOT EXISTS metrics_history_partitioned (
    LIKE metrics_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS
) PARTITION BY RANGE (date);

-- Create monthly partitions
DO $$
DECLARE
    start_date date;
    end_date date;
    partition_name text;
BEGIN
    FOR i IN 0..11 LOOP
        start_date = date_trunc('month', CURRENT_DATE)::date + (i || ' months')::interval;
        end_date = start_date + interval '1 month';
        partition_name = 'metrics_history_' || to_char(start_date, 'YYYY_MM');
        
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I PARTITION OF metrics_history_partitioned
            FOR VALUES FROM (%L) TO (%L)
        ', partition_name, start_date, end_date);
    END LOOP;
END $$;
*/

-- =====================================================
-- STATISTICS AND ANALYTICS
-- =====================================================

-- Update statistics for query planner
ANALYZE daily_insights;
ANALYZE recommendations;
ANALYZE metrics_history;
ANALYZE scheduled_jobs;
ANALYZE insight_feedback;

-- =====================================================
-- CLEANUP FUNCTION (for old data)
-- =====================================================

CREATE OR REPLACE FUNCTION cleanup_old_insights(retention_days INTEGER DEFAULT 365)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    WITH deleted AS (
        DELETE FROM daily_insights
        WHERE date < CURRENT_DATE - retention_days
        AND deleted_at IS NULL
        RETURNING id
    )
    SELECT COUNT(*) INTO deleted_count FROM deleted;
    
    -- Also clean up orphaned recommendations
    DELETE FROM recommendations
    WHERE insight_id NOT IN (SELECT id FROM daily_insights);
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- GRANTS (for production security)
-- =====================================================

-- Adjust based on your security requirements
-- GRANT SELECT, INSERT, UPDATE ON daily_insights TO insight_app;
-- GRANT SELECT ON account_insight_summary TO reporting_user;
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO insight_admin;

-- =====================================================
-- COMMENTS
-- =====================================================

COMMENT ON TABLE daily_insights IS 'Daily business insights generated for accounts';
COMMENT ON TABLE recommendations IS 'Actionable recommendations derived from insights';
COMMENT ON TABLE metrics_history IS 'Historical metric data for trend analysis';
COMMENT ON TABLE scheduled_jobs IS 'Background job scheduling and tracking';
COMMENT ON TABLE insight_feedback IS 'User feedback on insight quality';

COMMENT ON COLUMN daily_insights.metrics IS 'JSON object containing key business metrics';
COMMENT ON COLUMN daily_insights.key_events IS 'JSON array of important events';
COMMENT ON COLUMN daily_insights.recommendations IS 'JSON array of recommendations';
COMMENT ON COLUMN daily_insights.sentiment IS 'Business sentiment score (-1 to 1)';
COMMENT ON COLUMN daily_insights.confidence IS 'Confidence in insight accuracy (0 to 1)';
COMMENT ON COLUMN daily_insights.tags IS 'Array of tags for categorization';
COMMENT ON COLUMN daily_insights.deleted_at IS 'Soft delete timestamp';

COMMENT ON COLUMN scheduled_jobs.attempts IS 'Number of execution attempts';
COMMENT ON COLUMN scheduled_jobs.max_attempts IS 'Maximum allowed attempts';
COMMENT ON COLUMN scheduled_jobs.duration_ms IS 'Execution duration in milliseconds';

-- =====================================================
-- MAINTENANCE FUNCTIONS
-- =====================================================

-- Function to schedule daily insight generation
CREATE OR REPLACE FUNCTION schedule_daily_insights()
RETURNS void AS $$
BEGIN
    INSERT INTO scheduled_jobs (
        job_type,
        scheduled_at,
        status,
        priority
    )
    SELECT 
        'DAILY_INSIGHT_GENERATION'::job_type,
        CURRENT_DATE + interval '1 day' + interval '1 hour', -- 1 AM next day
        'PENDING'::job_status,
        1 -- High priority
    WHERE NOT EXISTS (
        SELECT 1 FROM scheduled_jobs
        WHERE job_type = 'DAILY_INSIGHT_GENERATION'
        AND DATE(scheduled_at) = CURRENT_DATE + 1
    );
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- INITIAL DATA (if needed)
-- =====================================================

-- Create default job schedule
INSERT INTO scheduled_jobs (
    job_type,
    scheduled_at,
    status,
    priority,
    metadata
) VALUES 
    ('DAILY_INSIGHT_GENERATION'::job_type, 
     CURRENT_DATE + interval '1 day' + interval '1 hour',
     'PENDING'::job_status,
     1,
     '{"description": "Daily insight generation for all accounts"}'::JSONB),
    ('DATA_CLEANUP'::job_type,
     CURRENT_DATE + interval '1 day' + interval '2 hours',
     'PENDING'::job_status,
     3,
     '{"description": "Clean up old data and optimize tables"}'::JSONB),
    ('CACHE_WARMUP'::job_type,
     CURRENT_DATE + interval '1 day' + interval '30 minutes',
     'PENDING'::job_status,
     2,
     '{"description": "Warm up Redis caches for common queries"}'::JSONB)
ON CONFLICT DO NOTHING;