# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /build

# Pre-fetch dependencies for layer caching
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real application
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libgcc-s1 \
        gcc \
        g++ \
        python3 \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/rust-judge /app/rust-judge

# Create working and file-store directories
RUN mkdir -p /tmp/rust-judge/files

EXPOSE 5050

ENV HTTP_ADDR=0.0.0.0:5050 \
    WORK_DIR=/tmp/rust-judge \
    LOG_LEVEL=info

ENTRYPOINT ["/app/rust-judge"]
