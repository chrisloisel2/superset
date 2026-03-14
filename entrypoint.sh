#!/bin/bash
set -e

case "${SERVICE_NAME}" in

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

  *)
    exec gunicorn \
      --bind 0.0.0.0:8088 \
      --workers 4 \
      --worker-class gevent \
      --timeout 300 \
      --keep-alive 5 \
      "superset.app:create_app()"
    ;;
esac
