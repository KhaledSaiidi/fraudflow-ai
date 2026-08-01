# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

WORKDIR /app

ARG LIBRDKAFKA_VERSION=2.15.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    gcc \
    g++ \
    build-essential \
    python3-dev \
    libssl-dev \
    libsasl2-dev \
    pkg-config \
    && rm -rf /vat/lib/apt/lists/*

RUN curl -fsSL "https://github.com/confluentinc/librdkafka/archive/refs/tags/v${LIBRDKAFKA_VERSION}.tar.gz" \
    -o /tmp/librdkafka.tar.gz && \
    tar -xzf /tmp/librdkafka.tar.gz -C /tmp && \
    cd "/tmp/librdkafka-${LIBRDKAFKA_VERSION}" && \
    ./configure --enable-ssl --enable-sasl && \
    make -j"$(nproc)" && \
    make install && \
    ldconfig && \
    rm -rf \
    "/tmp/librdkafka-${LIBRDKAFKA_VERSION}" \
    /tmp/librdkafka.tar.gz \
    /var/lib/apt/lists/*

COPY ./requirements.txt /app/requirements.txt

COPY . .

ENTRYPOINT [ "python3", "main.py" ]