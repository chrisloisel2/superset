#!/bin/bash
set -e

case "${SERVICE_NAME}" in

  # ── Hive Metastore ──────────────────────────────────────────────────────────
  metastore)
    /opt/init-tez-hdfs.sh &
    exec /entrypoint.sh
    ;;

  # ── HiveServer2 ─────────────────────────────────────────────────────────────
  hiveserver2)
    /opt/init-tez-hdfs.sh &
    # Injecter metastore URI une seule fois
    if ! grep -q "hive.metastore.uris" /opt/hive/conf/hive-site.xml 2>/dev/null; then
      sed -i 's|</configuration>|  <property><name>hive.metastore.uris</name><value>thrift://hive-metastore:9083</value></property>\n</configuration>|' \
          /opt/hive/conf/hive-site.xml
    fi
    exec /entrypoint.sh
    ;;

  # ── Superset init (db upgrade + admin) ──────────────────────────────────────
  superset-init)
    superset db upgrade
    superset fab create-admin \
      --username admin \
      --firstname Admin \
      --lastname SuperHive \
      --email admin@hadoop.red \
      --password "${SUPERSET_ADMIN_PASSWORD:-Admin123456}" 2>/dev/null || true
    superset init
    ;;

  # ── Superset webserver (défaut) ──────────────────────────────────────────────
  *)
    exec gunicorn \
      --bind 0.0.0.0:8088 \
      --workers 4 \
      --worker-class gevent \
      --timeout 300 \
      --keep-alive 5 \
      --limit-request-line 0 \
      --limit-request-field_size 0 \
      "superset.app:create_app()"
    ;;
esac
