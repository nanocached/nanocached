FROM rust:1.96.0-alpine3.21 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --locked --release

FROM alpine:3.21

RUN addgroup --system kvelo \
    && adduser --system --ingroup kvelo kvelo

COPY --from=builder /app/target/release/kvelo /usr/local/bin/kvelo

USER 10001:10001

EXPOSE 8356

ENTRYPOINT ["kvelo"]
CMD ["--host", "0.0.0.0"]
