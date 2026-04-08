# ═══════════════════════════════════════════════════════════════════════════════
# SuperHive — Superset 3.1.3
# Drivers : Hive (pyhive/NOSASL) · AWS S3 (boto3/s3fs) · MongoDB · PostgreSQL
# Spool : 192.168.88.5 · Embed : 192.168.88.27:23000
# ═══════════════════════════════════════════════════════════════════════════════
FROM apache/superset:3.1.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ python3-dev \
        libsasl2-dev libsasl2-modules \
        libssl-dev libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY superset_config.py /app/pythonpath/superset_config.py
COPY scripts/ /app/scripts/
COPY entrypoint.sh /superhive-entrypoint.sh
RUN chmod +x /superhive-entrypoint.sh && \
    mkdir -p /app/superset_home/uploads && \
    chown -R superset:superset /app/superset_home

USER superset

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

ENTRYPOINT ["/superhive-entrypoint.sh"]
