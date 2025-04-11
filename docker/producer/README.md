# GPS Producer

A Python application that simulates GPS data and sends it to Kafka.

## Architecture

The application follows SOLID principles and is organized into the following modules:

- **config**: Configuration loading and management
- **data**: Data handling, including GPS data loading and vehicle processing
- **kafka**: Kafka integration for sending messages
- **utils**: Utility functions, including signal handling

### Key Components

- **ConfigLoader**: Loads and provides access to configuration settings
- **GPSDataLoader**: Loads and validates GPS data from CSV files
- **KafkaProducerService**: Handles Kafka producer operations
- **VehicleProcessor**: Processes vehicle GPS data
- **GPSSimulator**: Simulates real-time GPS data with concurrent processing
- **SignalHandler**: Handles system signals for graceful shutdown
- **GPSProducerApp**: Main application class that ties everything together

## Setup

1. Ensure you have Docker and Docker Compose installed
2. Place your GPS data CSV file in the `data` directory
3. Configure the application in `config/config.yaml`
4. Run the application with Docker Compose:

```bash
docker-compose up --build
```

## Configuration

The application is configured via the `config/config.yaml` file:

```yaml
kafka:
  bootstrap_servers: 'kafka:9092'
  topic: 'gps-data'
  api_version: [2, 5, 0]
  retry:
    max_attempts: 30
    delay_seconds: 2

producer:
  csv_file: '/data/raw_gps.csv'
  message_delay:
    min: 10
    max: 20
  logging:
    level: 'INFO'
    format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
```

## Development

To extend or modify the application:

1. Follow the existing module structure
2. Ensure new components follow SOLID principles
3. Add appropriate tests
4. Update documentation as needed 