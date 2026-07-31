# syntax=docker/dockerfile:1

FROM python:3.11-slim-bookworm AS builder

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

RUN python -m venv "${VIRTUAL_ENV}"

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
        --requirement /tmp/requirements.txt \
    && pip check

FROM python:3.11-slim-bookworm

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN groupadd --system --gid 10001 mlflow \
    && useradd --system --uid 10001 --gid mlflow \
       --home-dir /nonexistent --shell /usr/sbin/nologin mlflow

COPY --from=builder /opt/venv /opt/venv

USER 10001:10001

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/health', timeout=3).close()"]

ENTRYPOINT ["mlflow"]
CMD ["server", "--host=0.0.0.0", "--port=5000"]
