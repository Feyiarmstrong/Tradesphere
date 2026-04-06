FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ./ingestion/
COPY delta/ ./delta/
COPY spark/ ./spark/
COPY snowflake/ ./snowflake/
COPY validation/ ./validation/

CMD ["python", "ingestion/main.py"]