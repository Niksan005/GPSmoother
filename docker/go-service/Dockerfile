FROM golang:1.24-alpine

WORKDIR /app

# Copy go mod and sum files
COPY src/go.mod src/go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY src/ .

# Build the application
RUN go build -o gpsmoother ./cmd/gpsmoother

# Run the application
CMD ["./gpsmoother"] 