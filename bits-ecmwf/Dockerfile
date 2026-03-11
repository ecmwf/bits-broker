###############################
# Stage 1: Prepare Cargo Chef Recipe
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS chef
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y pkg-config build-essential python3 python3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

###############################
# Stage 2: Cache Dependencies
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS cacher
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y pkg-config build-essential python3 python3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=chef /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

###############################
# Stage 3: Build the Project
###############################
FROM docker.io/library/rust:1.90-slim-bookworm AS builder
RUN apt-get update && apt-get install -y pkg-config build-essential python3 python3-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN cargo build --release -p bits-ecmwf --bin bits-ecmwf

###############################
# Stage 4: Production Release Image
###############################
FROM gcr.io/distroless/cc-debian12:nonroot AS release
ARG VERSION
LABEL version=$VERSION
WORKDIR /app
COPY --from=builder /app/target/release/bits-ecmwf /app/bits-ecmwf
ENTRYPOINT ["/app/bits-ecmwf"]

###############################
# Stage 5: Debug Image
###############################
FROM docker.io/library/debian:bookworm-slim AS debug
ARG VERSION
LABEL version=$VERSION
RUN apt-get update && apt-get install -y ca-certificates bash && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/bits-ecmwf /app/bits-ecmwf
ENTRYPOINT ["/app/bits-ecmwf"]
