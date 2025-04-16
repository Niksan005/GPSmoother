# GPSmoother

A Go-based project for GPS data processing and analysis.

## Prerequisites

- Go 1.x (latest stable version recommended)
- Docker and Docker Compose
- Python 3.x (for data conversion)
- OSRM (Open Source Routing Machine)

## Project Setup

### 1. Initialize OSRM

First, you need to set up OSRM for routing data:

```bash
# Download and extract OSRM data
cd data

docker-compose -f data/osrm.yml --profile init --profile app up -d
```

### 2. Start Kafka Services

```bash
cd docker/kafka
docker-compose up -d
```

### 3. Create Kafka Topics

After Kafka is running, create the required topics:

```bash
docker exec -it kafka kafka-topics.sh --create --topic gps-data --bootstrap-server localhost:9092 --partitions 10 --replication-factor 1
docker exec -it kafka kafka-topics.sh --create --topic smooth-gps-data --bootstrap-server localhost:9092 --partitions 10 --replication-factor 1
```

### 4. Start the Producer

```bash
cd docker/producer
docker-compose up -d
```

### 5. Start the Processor

```bash
cd docker/processor
docker-compose up -d
```

## Testing and Data Export

### 1. Export Data from Kafka

To export the processed GPS data:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic smooth-gps-data \
  --property print.key=true \
  --property key.separator=":" \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --from-beginning | grep "^0040EDF8:" > messages.txt
```

### 2. Convert Data to CSV

A Python script is provided to convert the exported messages to CSV format:

```bash
python3 scripts/convert_to_csv.py messages.txt output.csv
```

The conversion script (`scripts/convert_to_csv.py`) will be created with the following content:

## Project Structure

```
.
├── data/               # OSRM data and processed files
├── docker/
│   ├── kafka/         # Kafka Docker configuration
│   └── producer/      # Producer Docker configuration
├── processor/         # GPS data processing code
├── scripts/          # Utility scripts
└── messages.txt      # Exported Kafka messages
```

## Development

[Add development-specific instructions here]

## Contributing

[Add contribution guidelines here]

## License

[Add license information here]
