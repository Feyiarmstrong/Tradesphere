FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY ingestion/ ./ingestion/
COPY delta/ ./delta/
COPY spark/ ./spark/
COPY snowflake/ ./snowflake/
COPY validation/ ./validation/
COPY config/ ./config/
COPY .env .

# Default command runs the full ingestion pipeline
CMD ["python", "ingestion/main.py"]