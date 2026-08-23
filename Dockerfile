FROM rust:1.96.0-alpine3.21 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --locked --release --bin nanocached-node --bin nanocached-discovery --bin nanocached-proxy

FROM alpine:3.21 AS node

RUN addgroup -g 10001 -S nanocached \
    && adduser -u 10001 -S -G nanocached nanocached

COPY --from=builder /app/target/release/nanocached-node /usr/local/bin/nanocached-node

USER 10001:10001

EXPOSE 8356

ENTRYPOINT ["nanocached-node"]
CMD ["--host", "0.0.0.0"]

FROM alpine:3.21 AS discovery

RUN addgroup -g 10001 -S nanocached \
    && adduser -u 10001 -S -G nanocached nanocached

COPY --from=builder /app/target/release/nanocached-discovery /usr/local/bin/nanocached-discovery

USER 10001:10001

EXPOSE 8357

ENTRYPOINT ["nanocached-discovery"]
CMD ["--host", "0.0.0.0"]

FROM alpine:3.21 AS proxy

RUN addgroup -g 10001 -S nanocached \
    && adduser -u 10001 -S -G nanocached nanocached

COPY --from=builder /app/target/release/nanocached-proxy /usr/local/bin/nanocached-proxy

USER 10001:10001

EXPOSE 8358

ENTRYPOINT ["nanocached-proxy"]
CMD ["--host", "0.0.0.0"]
