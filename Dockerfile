# Base image and version — verified in practice (docker pull rust:1-slim;
# docker run --rm rust:1-slim rustc --version): rustc 1.98.0 on Debian 13
# (trixie), which is above the Cargo.toml requirement rust-version = "1.95".
# The runtime image (debian:stable-slim) is also trixie — the same glibc
# version as the build image, so the copied binary won't hit an
# incompatibility.
FROM rust:1-slim AS build
WORKDIR /src
# Dependency layer: manifests + stub files for both package targets (lib.rs
# and main.rs — DECISIONS Q24, "one crate, lib + thin bin") are built
# separately, BEFORE the real src/ lands in the image. As long as
# Cargo.toml and Cargo.lock haven't changed, Docker reuses this whole layer,
# and cargo inside the next RUN only rebuilds pgcdc itself (two small
# files), not all the external crates again. Measured by editing one line
# in src/main.rs and re-running `docker build`: 40.5s from scratch (36.4s —
# this stub layer) → 3.6s on a rebuild (dependency layer CACHED,
# task-4-report.md).
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release --bin pgcdc \
    && rm -rf src
# The real src/ arrives with an mtime newer than the stubs — but COPY on
# some BuildKit engines can preserve the original mtime instead of the copy
# time, and cargo triggers a rebuild based on mtime among other things — so
# touch is not optional, it's a required step, not a belt-and-suspenders
# precaution.
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --bin pgcdc

FROM debian:stable-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/pgcdc /usr/local/bin/pgcdc
# Logs go to stderr, the payload goes to stdout, so the container's output
# can be piped downstream without filtering.
ENTRYPOINT ["/usr/local/bin/pgcdc"]
