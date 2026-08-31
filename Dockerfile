# Базовый образ и версия — фактически проверены (docker pull rust:1-slim;
# docker run --rm rust:1-slim rustc --version): rustc 1.98.0 на Debian 13
# (trixie), что выше требуемых Cargo.toml rust-version = "1.95". Образ
# рантайма (debian:stable-slim) тоже trixie — та же версия glibc, что и в
# образе сборки, поэтому скопированный бинарь не упрётся в несовместимость.
FROM rust:1-slim AS build
WORKDIR /src
# Сначала манифесты: слой с зависимостями переиспользуется, пока они не менялись.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin pgcdc

FROM debian:stable-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/pgcdc /usr/local/bin/pgcdc
# Логи идут в stderr, полезная нагрузка — в stdout, поэтому вывод контейнера
# можно направлять в конвейер без фильтрации.
ENTRYPOINT ["/usr/local/bin/pgcdc"]
