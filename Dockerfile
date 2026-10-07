# syntax=docker/dockerfile:1

# Explicit first-checkout bootstrap; this stage does not compile the server.
# Export only Cargo.lock with --target lockfile --output type=local,dest=.
FROM rust:1.85.1-bookworm AS lock-generator
WORKDIR /build
COPY Cargo.toml rust-toolchain.toml ./
COPY .cargo/ .cargo/
COPY crates/ crates/
RUN cargo generate-lockfile

FROM scratch AS lockfile
COPY --from=lock-generator /build/Cargo.lock /Cargo.lock

FROM rust:1.85.1-bookworm AS build
WORKDIR /build
RUN rustup component add rustfmt clippy

# A real reviewed lock must exist; do not silently resolve dependencies here.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo/ .cargo/
COPY crates/ crates/
RUN cargo build --locked --release -p recall-server

# Formatting and static-diagnostics gate; ordinary runtime builds do not run it.
# Apply formatting on the source checkout itself (cargo fmt), not via an export stage.
FROM build AS lint
RUN cargo fmt --all -- --check \
 && cargo clippy --workspace --all-targets --locked -- -D warnings

# Explicit verification target; ordinary runtime builds do not execute tests.
FROM lint AS test
RUN cargo test --workspace --locked

FROM debian:bookworm-slim AS runtime
LABEL org.opencontainers.image.title="Recall" \
      org.opencontainers.image.description="Multithreaded in-memory cache server built in Rust" \
      org.opencontainers.image.vendor="Coheret" \
      org.opencontainers.image.source="https://github.com/Coheret/recall"
WORKDIR /app
RUN mkdir -p /etc/recall && chmod 0755 /etc/recall
COPY --from=build /build/target/release/recall-server /usr/local/bin/recall-server
USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/recall-server"]
CMD ["--no-env-file"]
