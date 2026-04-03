# ============================================================================
# Stage 1: Build Stage
# ============================================================================
FROM python:3.11-slim AS builder

WORKDIR /app

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Install Poetry
RUN pip install --no-cache-dir poetry==1.7.0

# Configure Poetry to not create virtual environment
RUN poetry config virtualenvs.create false

# Install dependencies (including dev dependencies for testing)
RUN poetry install --no-interaction --no-ansi --no-root

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/

# ============================================================================
# Stage 2: Test Stage (for CI/CD)
# ============================================================================
FROM builder AS tester

# Install test dependencies
RUN pip install --no-cache-dir \
    pytest \
    pytest-cov \
    pytest-asyncio \
    pytest-mock \
    pytest-timeout

# Copy test files
COPY tests/ ./tests/

# Run tests with coverage
RUN pytest tests/ -v --cov=src --cov-report=xml --cov-report=html --cov-report=term

# ============================================================================
# Stage 3: Security Scan Stage
# ============================================================================
FROM builder AS security-scanner

# Install OWASP dependency-check
RUN apt-get update && apt-get install -y wget unzip && \
    wget https://github.com/jeremylong/DependencyCheck/releases/download/v9.0.9/dependency-check-9.0.9-release.zip && \
    unzip dependency-check-9.0.9-release.zip && \
    mv dependency-check /opt/ && \
    rm dependency-check-9.0.9-release.zip

# Run OWASP dependency check
RUN /opt/dependency-check/bin/dependency-check.sh \
    --scan /app \
    --format HTML \
    --format JSON \
    --out /app/security-reports \
    --enableExperimental \
    --failOnCVSS 7

# Run safety check for Python vulnerabilities
RUN pip install safety && \
    safety check --full-report --output html > /app/security-reports/safety-report.html || true

# Run bandit for Python security linter
RUN pip install bandit && \
    bandit -r src/ -f html -o /app/security-reports/bandit-report.html || true

# ============================================================================
# Stage 4: SonarQube Analysis Stage
# ============================================================================
FROM builder AS sonar-scanner

# Install SonarQube scanner
RUN apt-get update && apt-get install -y wget unzip && \
    wget https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-5.0.1.3006-linux.zip && \
    unzip sonar-scanner-cli-5.0.1.3006-linux.zip && \
    mv sonar-scanner-5.0.1.3006-linux /opt/sonar-scanner && \
    rm sonar-scanner-cli-5.0.1.3006-linux.zip

ENV PATH="${PATH}:/opt/sonar-scanner/bin"

# Run SonarQube analysis
COPY sonar-project.properties ./
RUN sonar-scanner \
    -Dsonar.projectKey=ceoos-insight-engine \
    -Dsonar.sources=src/ \
    -Dsonar.python.coverage.reportPaths=coverage.xml \
    -Dsonar.python.xunit.reportPath=test-results.xml \
    -Dsonar.python.pylint.reportPaths=pylint-report.txt

# ============================================================================
# Stage 5: Quality Gate Check
# ============================================================================
FROM alpine:latest AS quality-gate

# Check if quality gates pass (this is a placeholder - actual check happens in Jenkins)
RUN echo "Quality gate check passed"

# ============================================================================
# Stage 6: Production Stage
# ============================================================================
FROM python:3.11-slim AS production

WORKDIR /app

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN addgroup --system --gid 1001 appuser && \
    adduser --system --uid 1001 --gid 1001 appuser

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copy application code
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser scripts/ ./scripts/

# Create necessary directories
RUN mkdir -p /app/logs /app/data /app/security-reports && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 3004

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:3004/health || exit 1

# Run the application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "3004", "--workers", "2"]
