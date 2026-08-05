# Stage 1: Build the Rust binary
# We use a multi-stage build: first compile in a full Rust image,
# then copy just the binary into a tiny image. This keeps the
# final image small (no compiler, no source code, no build tools).
FROM rust:1.97 AS builder

# Create a working directory inside the container
WORKDIR /app

# Copy the source code into the container
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

# Build in release mode (optimized, no debug info)
# --release makes the binary much faster and smaller
RUN cargo build --release

# Stage 2: Create the final, minimal image
# debian:bookworm-slim is a tiny Linux image (~80MB vs ~1.5GB for rust:)
FROM debian:bookworm-slim

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy just the compiled binary from the builder stage
COPY --from=builder /app/target/release/raftkv /usr/local/bin/raftkv

# Create a data directory for the database
RUN mkdir -p /data

# The entry point: what runs when the container starts
# CMD vs ENTRYPOINT: ENTRYPOINT is the fixed command,
# CMD provides default arguments that can be overridden.
ENTRYPOINT ["raftkv"]