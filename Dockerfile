FROM rust:slim-bullseye AS buildstage
WORKDIR /build
ENV PROTOC_NO_VENDOR 1
COPY . /build/
RUN rustup component add rustfmt && \
    apt-get update && \
    apt-get install -y --no-install-recommends wget protobuf-compiler && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    GRPC_HEALTH_PROBE_VERSION=v0.4.10 && \
    wget -qO /bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe
RUN cargo build --release

FROM debian:bullseye-slim
RUN useradd -m chain
USER chain
COPY --from=buildstage /build/target/release/consensus /usr/bin/
COPY --from=buildstage /bin/grpc_health_probe /usr/bin/
CMD ["consensus"]
