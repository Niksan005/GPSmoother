.PHONY: build run test clean

# Build the application
build:
	go build -o bin/processor ./cmd/processor

# Run the application
run:
	go run ./cmd/processor

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	rm -rf bin/
	go clean

# Install dependencies
deps:
	go mod tidy
	go mod download

# Run linter
lint:
	golangci-lint run

# Run all checks
check: lint test 