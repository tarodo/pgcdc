# Базовый образ и версия — фактически проверены (docker pull rust:1-slim;
# docker run --rm rust:1-slim rustc --version): rustc 1.98.0 на Debian 13
# (trixie), что выше требуемых Cargo.toml rust-version = "1.95". Образ
# рантайма (debian:stable-slim) тоже trixie — та же версия glibc, что и в
# образе сборки, поэтому скопированный бинарь не упрётся в несовместимость.
FROM rust:1-slim AS build
WORKDIR /src
# Слой зависимостей: манифесты + заглушки обеих целей пакета (lib.rs и
# main.rs — DECISIONS Q24, "один крейт, lib + тонкий bin") собираются
# отдельно, ДО того как в образ попадает настоящий src/. Пока Cargo.toml и
# Cargo.lock не менялись, Docker переиспользует этот слой целиком, и cargo
# внутри следующего RUN пересобирает только сам pgcdc (два маленьких файла),
# а не все внешние крейты заново. Измерено правкой одной строки в src/main.rs
# и повторным `docker build`: 40.5с с нуля (36.4с — этот слой заглушек) →
# 3.6с на пересборке (слой зависимостей CACHED, task-4-report.md).
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release --bin pgcdc \
    && rm -rf src
# Настоящий src/ приходит с mtime свежее заглушек — но COPY в некоторых
# движках BuildKit может сохранить исходный mtime вместо времени копирования,
# а cargo триггерит пересборку в том числе по mtime, — поэтому touch не
# опция, а обязательный шаг, а не подстраховка ради подстраховки.
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --bin pgcdc

FROM debian:stable-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/pgcdc /usr/local/bin/pgcdc
# Логи идут в stderr, полезная нагрузка — в stdout, поэтому вывод контейнера
# можно направлять в конвейер без фильтрации.
ENTRYPOINT ["/usr/local/bin/pgcdc"]
