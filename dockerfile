# Base Python image (slim = smaller & faster)
FROM python:3.11-slim

# Environment settings:
# - Disable stdout buffering (real-time logs)
# - Prevent .pyc files inside container
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set working directory inside container
# All relative paths resolve from /app
WORKDIR /app

# Install system-level dependencies required
# for Python packages with native extensions
RUN apt-get update && apt-get install -y gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# (copied first to leverage Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code into container
COPY . .

# Container entry point:
# Runs the trading bot as a long-running process
CMD ["python", "src/algo_trader_main.py"]
