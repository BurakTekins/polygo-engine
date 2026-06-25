FROM rust:1.96-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin polygo-engine

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/target/release/polygo-engine /usr/local/bin/polygo-engine
COPY config.docker.json /app/config.json
EXPOSE 8090
ENTRYPOINT ["polygo-engine"]
