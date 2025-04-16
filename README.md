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

docker compose -f ./osrm.yml --profile init up --build
docker compose -f ./osrm.yml --profile app up --build
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

```python
import csv
import json

def convert_to_csv(input_file, output_file):
    with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        csv_writer = csv.writer(f_out)
        # Write header
        csv_writer.writerow(['timestamp', 'latitude', 'longitude', 'speed'])

        for line in f_in:
            try:
                # Split the line into key and value
                _, value = line.strip().split(':', 1)
                data = json.loads(value)
                csv_writer.writerow([
                    data.get('timestamp', ''),
                    data.get('latitude', ''),
                    data.get('longitude', ''),
                    data.get('speed', '')
                ])
            except Exception as e:
                print(f"Error processing line: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python convert_to_csv.py input.txt output.csv")
        sys.exit(1)

    convert_to_csv(sys.argv[1], sys.argv[2])
```

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
