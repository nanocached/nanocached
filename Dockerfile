FROM rust:1.96.0-alpine3.21 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --locked --release --bin ncd --bin nanocached-node --bin nanocached-discovery

FROM alpine:3.21

RUN addgroup --system nanocached \
    && adduser --system --ingroup nanocached nanocached

COPY --from=builder /app/target/release/ncd /usr/local/bin/ncd
COPY --from=builder /app/target/release/nanocached-node /usr/local/bin/nanocached-node
COPY --from=builder /app/target/release/nanocached-discovery /usr/local/bin/nanocached-discovery

USER 10001:10001

# 8356: cache node, 8357: discovery server. A single image plays either
# role, selected by CMD/args -- run the two roles as separate containers.
EXPOSE 8356 8357

ENTRYPOINT ["ncd"]
CMD ["node", "start", "--host", "0.0.0.0"]
