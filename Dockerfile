# Stage 1: Build statically linked binary
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Install git/certs if needed
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source files
COPY . .

# Build statically linked single binary with stripped debug symbols
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /bin/kessel-box ./cmd/kessel-box

# Stage 2: Minimal runtime image (< 25MB)
FROM gcr.io/distroless/static:nonroot

WORKDIR /app

# Copy binary from builder
COPY --from=builder /bin/kessel-box /app/kessel-box

# Expose gRPC (9000) and HTTP Developer Dashboard (8088)
EXPOSE 9000 8088

# Run as non-root
USER 65532:65532

ENTRYPOINT ["/app/kessel-box"]
CMD ["start", "--grpc-port=9000", "--http-port=8088", "--db-path=:memory:"]
