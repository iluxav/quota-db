# Build stage
FROM rust:1.83-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy everything needed for the build
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY benches ./benches

# Build the release binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/quota-db /usr/local/bin/quota-db

# Expose default ports
EXPOSE 6380 9090

# Default command with metrics enabled
ENTRYPOINT ["quota-db"]
CMD ["--bind", "0.0.0.0:6380", "--metrics", "--metrics-port", "9090"]
