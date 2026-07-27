# ==========================================
# STAGE 1: Builder
# ==========================================
FROM python:3.10-slim AS builder

WORKDIR /app

# Install system dependencies needed for compiling packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set up a virtual environment to isolate dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ==========================================
# STAGE 2: Runner (Production)
# ==========================================
FROM python:3.10-slim AS runner

WORKDIR /app

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create a non-root user for security
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -m -s /bin/bash appuser

# Copy application files
COPY --chown=appuser:appgroup scripts /app/scripts
COPY --chown=appuser:appgroup dashboard /app/dashboard
COPY --chown=appuser:appgroup src /app/src

# Switch to non-root user
USER appuser

# Expose Streamlit default port
EXPOSE 8501

# Default command to run the dashboard
CMD ["streamlit", "run", "dashboard/Home.py", "--server.port=8501", "--server.address=0.0.0.0"]
