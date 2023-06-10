FROM rust:1.70 as builder
WORKDIR /usr/src/torrentsnail
COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
RUN apt-get update && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/torrentsnail /usr/local/bin/torrentsnail
CMD ["torrentsnail"]