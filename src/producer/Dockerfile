# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /app

RUN python -m venv "${VIRTUAL_ENV}" \
    && groupadd --gid 10001 producer \
    && useradd --uid 10001 --gid producer --no-create-home \
       --home-dir /nonexistent --shell /usr/sbin/nologin producer

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install --requirement requirements.txt \
    && pip check

COPY main.py .

USER 10001:10001

ENTRYPOINT ["python", "main.py"]
