FROM golang:1.23-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git

COPY go.mod go.sum .
RUN go mod download

COPY *.go .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o worker .
# Final stage - minimal image
FROM alpine:3.20

WORKDIR /app

# Install ca-certificates for HTTPS requests
RUN apk add --no-cache ca-certificates

# Copy binary from builder
COPY --from=builder /app/worker .

# Expose metrics port
EXPOSE 9000

# Run the worker
CMD ["./worker"]
