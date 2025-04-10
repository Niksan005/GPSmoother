FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create necessary directories
RUN mkdir -p /data /app/config

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Copy the producer script
COPY producer.py .

# The CSV file will be mounted at runtime
CMD ["python", "producer.py"] 