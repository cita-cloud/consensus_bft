FROM rust:slim-buster AS buildstage
WORKDIR /build
COPY . /build/
RUN /bin/sh -c set -eux;\
    rustup component add rustfmt;\
    apt-get update;\
    apt-get install -y --no-install-recommends make git protobuf-compiler libssl-dev pkg-config clang build-essential libsnappy-dev;\
    rm -rf /var/lib/apt/lists/*;
RUN cargo build --release
FROM debian:buster-slim
COPY --from=buildstage /build/target/release/consensus_bft /usr/bin/
RUN /bin/sh -c set -eux;\
    apt-get update;\
    apt-get install -y --no-install-recommends libssl1.1;\
    rm -rf /var/lib/apt/lists/*;
CMD ["RUST_LOG=consensus_bft=trace consensus_bft"]
