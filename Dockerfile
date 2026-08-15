FROM rust:1.96.0-alpine3.21 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --locked --release --bin nanocached-node --bin nanocached-discovery

FROM alpine:3.21 AS node

RUN addgroup --system nanocached \
    && adduser --system --ingroup nanocached nanocached

COPY --from=builder /app/target/release/nanocached-node /usr/local/bin/nanocached-node

USER 10001:10001

EXPOSE 8356

ENTRYPOINT ["nanocached-node"]
CMD ["--host", "0.0.0.0"]

FROM alpine:3.21 AS discovery

RUN addgroup --system nanocached \
    && adduser --system --ingroup nanocached nanocached

COPY --from=builder /app/target/release/nanocached-discovery /usr/local/bin/nanocached-discovery

USER 10001:10001

EXPOSE 8357

ENTRYPOINT ["nanocached-discovery"]
CMD ["--host", "0.0.0.0"]
