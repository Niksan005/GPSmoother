# GPSmoother

A real-time streaming pipeline that ingests raw, noisy GPS data, cleans and smooths it using OSRM's road-snapping and routing capabilities, and outputs accurate, map-aligned coordinates.

## Project Structure

```
.
├── cmd/
│   └── gpsmoother/      # Main application entry point
├── internal/            # Private application code
│   ├── config/         # Configuration handling
│   ├── domain/         # Core business logic and types
│   ├── repository/     # Data access layer
│   ├── service/        # Business logic layer
│   └── transport/      # API handlers, HTTP, gRPC, etc.
├── pkg/                # Public library code
├── api/                # API contracts, OpenAPI/Swagger specs
├── configs/            # Configuration files
├── deployments/        # Deployment configurations
├── scripts/            # Build and maintenance scripts
├── test/               # Additional external test apps and data
├── docs/               # Documentation
├── examples/           # Example applications
└── tools/              # Supporting tools
```

## Prerequisites

- Go 1.24 or later
- Docker and Docker Compose
- Kafka (provided via Docker)

## Setup

1. Clone the repository:
```bash
git clone https://github.com/Niksan005/GPSmoother.git
cd GPSmoother
```

2. Start the required services:
```bash
cd docker/kafka
docker-compose up -d
```

3. Build and run the service:
```bash
go build -o gpsmoother ./cmd/gpsmoother
./gpsmoother
```

## Development

[Add development-specific instructions here]

## Testing

[Add testing instructions here]

## Contributing

[Add contribution guidelines here]

## License

MIT License 