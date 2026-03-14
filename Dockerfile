FROM apache/superset:3.1.3

USER root

# Dépendances système pour sasl / pyhive
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libsasl2-dev \
        libsasl2-modules \
        python3-dev \
        libldap2-dev \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Drivers Python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Configuration unique
COPY superset_config.py /app/pythonpath/superset_config.py

# Répertoire uploads
RUN mkdir -p /app/superset_home/uploads && \
    chown -R superset:superset /app/superset_home

USER superset

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
