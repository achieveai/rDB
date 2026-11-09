# rDB node image: one image, two roles (issue #85).
#   voter  -> config-server, the rEtcd voter (control plane)
#   data   -> rdb_node, one rDB data node (example of rdb-api)
# Build from the repository root:
#   docker build -f docker/Dockerfile -t rdb-bf-node:dev .
# See docker/README.md for how to run each role.
ARG RUST_IMAGE=rust:slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS build
# RocksDB (librocksdb-sys) compiles from source with a C++ compiler and runs bindgen, which
# loads libclang. libclang-dev provides the unversioned libclang.so that bindgen searches for;
# no LIBCLANG_PATH is set anywhere in this build.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential libclang-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p config-server \
 && cargo build --release --locked -p rdb-api --example rdb_node \
 && mkdir /out \
 && cp target/release/config-server target/release/examples/rdb_node /out/

FROM ${RUNTIME_IMAGE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends openssl curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --create-home --home-dir /data --uid 10001 rdb
COPY --from=build /out/config-server /out/rdb_node /usr/local/bin/
COPY docker/entrypoint.sh /usr/local/bin/rdb-entrypoint
RUN chmod 0755 /usr/local/bin/rdb-entrypoint
USER rdb
WORKDIR /data
VOLUME /data

EXPOSE 7001 7002 7003 7004
ENTRYPOINT ["rdb-entrypoint"]
CMD ["help"]
