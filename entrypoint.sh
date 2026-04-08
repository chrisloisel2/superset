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

    # ── Enregistrement base robotics (HDD sessions) dans Superset ────────────
    python3 - <<'PYEOF'
import os, time, requests

BASE     = "http://localhost:8088"
USER     = "admin"
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "Admin123456")

# Attendre que Superset soit prêt (max 60s)
for _ in range(12):
    try:
        r = requests.get(f"{BASE}/health", timeout=5)
        if r.status_code == 200:
            break
    except Exception:
        pass
    time.sleep(5)

session = requests.Session()

# Login
resp = session.post(f"{BASE}/api/v1/security/login", json={
    "username": USER, "password": PASSWORD,
    "provider": "db", "refresh": True,
})
token = resp.json().get("access_token")
if not token:
    print("Login Superset échoué, skip enregistrement dataset.")
    exit(0)

headers = {"Authorization": f"Bearer {token}"}

# CSRF token
csrf = session.get(f"{BASE}/api/v1/security/csrf_token/", headers=headers)
csrf_token = csrf.json().get("result")
headers["X-CSRFToken"] = csrf_token

# Vérifier si la DB robotics est déjà enregistrée
dbs = session.get(f"{BASE}/api/v1/database/", headers=headers).json()
existing = [d for d in dbs.get("result", []) if d.get("database_name") == "HDD Robotics"]

if not existing:
    payload = {
        "database_name": "HDD Robotics",
        "sqlalchemy_uri": "postgresql+psycopg2://superset:superset123@postgres:5432/robotics",
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_dml": False,
    }
    r = session.post(f"{BASE}/api/v1/database/", json=payload, headers=headers)
    if r.status_code in (200, 201):
        db_id = r.json()["id"]
        print(f"Base 'HDD Robotics' enregistrée (id={db_id})")
    else:
        print(f"Erreur enregistrement DB : {r.text}")
        exit(0)
else:
    db_id = existing[0]["id"]
    print(f"Base 'HDD Robotics' déjà présente (id={db_id})")

# Enregistrer les datasets
for table_name in ("hdd_sessions", "hdd_cameras", "hdd_trackers", "v_sessions_full"):
    datasets = session.get(
        f"{BASE}/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,val:{table_name})))",
        headers=headers,
    ).json()
    if datasets.get("count", 0) == 0:
        r = session.post(f"{BASE}/api/v1/dataset/", json={
            "database": db_id,
            "schema": "public",
            "table_name": table_name,
        }, headers=headers)
        if r.status_code in (200, 201):
            print(f"Dataset '{table_name}' enregistré.")
        else:
            print(f"Erreur dataset '{table_name}': {r.text}")
    else:
        print(f"Dataset '{table_name}' déjà présent.")
PYEOF
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
