# Multi-stage build for volant-server.
# Default image is plaintext (no TLS feature). Build with --build-arg FEATURES=tls for TLS.

FROM rust:1.83-bookworm AS builder
WORKDIR /src
COPY . .
ARG FEATURES=
RUN if [ -n "$FEATURES" ]; then \
      cargo build --release -p volant-server --features "$FEATURES"; \
    else \
      cargo build --release -p volant-server; \
    fi

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/volant --create-home volant
COPY --from=builder /src/target/release/volant-server /usr/local/bin/volant-server
USER volant
WORKDIR /var/lib/volant
EXPOSE 9092 9102
VOLUME ["/var/lib/volant/data"]
ENTRYPOINT ["volant-server"]
CMD ["--data-dir", "/var/lib/volant/data", "--listen", "0.0.0.0:9092", "--metrics-addr", "0.0.0.0:9102"]
