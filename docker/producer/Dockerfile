FROM python:3.9.18-slim

WORKDIR /app

# Install only the required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends netcat-traditional && \
    rm -rf /var/lib/apt/lists/*

# Copy application files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy configuration
COPY config/config.yaml /app/config/

# Copy source code
COPY src /app/src

# Create data directory and copy data file
RUN mkdir -p /data
COPY data/raw_gps.csv /data/

# Run the producer
CMD ["python", "-m", "src.main"] 