#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sftp_sync.py
============

Synchronise les metadata.json depuis un serveur SFTP vers PostgreSQL.

Objectifs :
- scan récursif des sessions via recherche de metadata.json
- séparation stricte des layers : inbox / bronze / silver / gold
- création automatique des tables et index PostgreSQL
- insertion progressive par batchs pour gros volumes
- enregistrement automatique de la base et des datasets dans Superset
- mode watch pour ingestion continue

Usage :
    python3 scripts/sftp_sync.py
    python3 scripts/sftp_sync.py --watch
    python3 scripts/sftp_sync.py --watch --interval 60
"""

import argparse
import json
import logging
import os
import posixpath
import time
from io import BytesIO
from stat import S_ISDIR

import paramiko
import psycopg2
import requests
from psycopg2.extras import execute_values

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration SFTP ────────────────────────────────────────────────────────

HDD_HOST = os.environ.get("HDD_HOST", "192.168.88.82")
HDD_PORT = int(os.environ.get("HDD_PORT", "22"))
HDD_USER = os.environ.get("HDD_USER", "exoria")
HDD_PASSWORD = os.environ.get("HDD_PASSWORD", "Admin123456")

LAYERS = [
    ("inbox", os.environ.get("HDD_INBOX", "/mnt/inbox")),
    ("bronze", os.environ.get("HDD_BRONZE", "/mnt/storage/bronze")),
    ("silver", os.environ.get("HDD_SILVER", "/mnt/storage/silver")),
    ("gold", os.environ.get("HDD_GOLD", "/mnt/storage/gold")),
]

# ── Configuration PostgreSQL ──────────────────────────────────────────────────

PG_DSN = os.environ.get(
    "ROBOTICS_DB_URI",
    "postgresql://superset:superset123@postgres:5432/robotics",
)

PG_BATCH_SIZE = int(os.environ.get("PG_BATCH_SIZE", "500"))
SFTP_MAX_DEPTH = int(os.environ.get("SFTP_MAX_DEPTH", "8"))

# ── Configuration Superset ────────────────────────────────────────────────────

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_PASSWORD", "Admin123456")
SUPERSET_DB_NAME = os.environ.get("SUPERSET_DB_NAME", "HDD Robotics")
SUPERSET_DB_URI = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://superset:superset123@postgres:5432/robotics",
)

# ── Helpers SFTP ──────────────────────────────────────────────────────────────

def sftp_connect() -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        HDD_HOST,
        port=HDD_PORT,
        username=HDD_USER,
        password=HDD_PASSWORD,
    )
    return ssh, ssh.open_sftp()


def sftp_read_json(sftp: paramiko.SFTPClient, path: str) -> dict | None:
    try:
        buf = BytesIO()
        sftp.getfo(path, buf)
        return json.loads(buf.getvalue().decode("utf-8"))
    except Exception as e:
        log.warning("Impossible de lire %s : %s", path, e)
        return None


def sftp_find_metadata_files(
    sftp: paramiko.SFTPClient,
    root: str,
    max_depth: int = 8,
) -> list[str]:
    """
    Recherche récursivement tous les metadata.json sous un root donné.
    Un appel = un seul layer.
    """
    results: list[str] = []

    def walk(current: str, depth: int) -> None:
        if depth > max_depth:
            return

        try:
            entries = sftp.listdir_attr(current)
        except Exception as e:
            log.warning("Impossible de lister %s : %s", current, e)
            return

        for entry in entries:
            name = entry.filename
            full_path = posixpath.join(current, name)

            try:
                if S_ISDIR(entry.st_mode):
                    walk(full_path, depth + 1)
                elif name == "metadata.json":
                    results.append(full_path)
            except Exception as e:
                log.warning("Erreur inspection %s : %s", full_path, e)

    walk(root, 0)
    return results


# ── Helpers PostgreSQL ────────────────────────────────────────────────────────

def pg_connect():
    return psycopg2.connect(PG_DSN)


def create_schema(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdd_sessions (
            id BIGSERIAL PRIMARY KEY,
            layer TEXT NOT NULL,
            session_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            scenario TEXT,
            start_time TIMESTAMPTZ NULL,
            end_time TIMESTAMPTZ NULL,
            trigger_time TIMESTAMPTZ NULL,
            duration_seconds DOUBLE PRECISION NULL,
            failed BOOLEAN DEFAULT FALSE,
            video_width INTEGER NULL,
            video_height INTEGER NULL,
            video_fps DOUBLE PRECISION NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            metadata_json JSONB NULL,
            CONSTRAINT uq_hdd_sessions_layer_session UNIQUE (layer, session_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdd_cameras (
            id BIGSERIAL PRIMARY KEY,
            layer TEXT NOT NULL,
            session_id TEXT NOT NULL,
            camera_key TEXT NOT NULL,
            name TEXT NULL,
            position TEXT NULL,
            serial TEXT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_hdd_cameras_layer_session_camera UNIQUE (layer, session_id, camera_key)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdd_trackers (
            id BIGSERIAL PRIMARY KEY,
            layer TEXT NOT NULL,
            session_id TEXT NOT NULL,
            tracker_key TEXT NOT NULL,
            serial TEXT NULL,
            model TEXT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_hdd_trackers_layer_session_tracker UNIQUE (layer, session_id, tracker_key)
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_hdd_sessions_layer
        ON hdd_sessions(layer)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_hdd_sessions_session_id
        ON hdd_sessions(session_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_hdd_sessions_start_time
        ON hdd_sessions(start_time)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_hdd_sessions_source_path
        ON hdd_sessions(source_path)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_hdd_sessions_metadata_json
        ON hdd_sessions USING GIN (metadata_json)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_hdd_cameras_layer_session
        ON hdd_cameras(layer, session_id)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_hdd_trackers_layer_session
        ON hdd_trackers(layer, session_id)
    """)

    cur.execute("""
        CREATE OR REPLACE VIEW v_sessions_full AS
        SELECT
            s.id,
            s.layer,
            s.session_id,
            s.source_path,
            s.scenario,
            s.start_time,
            s.end_time,
            s.trigger_time,
            s.duration_seconds,
            s.failed,
            s.video_width,
            s.video_height,
            s.video_fps,
            s.synced_at,
            s.metadata_json,
            COALESCE(c.cameras_count, 0) AS cameras_count,
            COALESCE(t.trackers_count, 0) AS trackers_count
        FROM hdd_sessions s
        LEFT JOIN (
            SELECT layer, session_id, COUNT(*) AS cameras_count
            FROM hdd_cameras
            GROUP BY layer, session_id
        ) c
            ON c.layer = s.layer
           AND c.session_id = s.session_id
        LEFT JOIN (
            SELECT layer, session_id, COUNT(*) AS trackers_count
            FROM hdd_trackers
            GROUP BY layer, session_id
        ) t
            ON t.layer = s.layer
           AND t.session_id = s.session_id
    """)


def parse_metadata(meta: dict, layer: str, source_path: str, fallback_session_id: str) -> dict:
    vc = meta.get("video_config") or {}

    return {
        "layer": layer,
        "session_id": meta.get("session_id") or fallback_session_id,
        "source_path": source_path,
        "scenario": meta.get("scenario"),
        "start_time": meta.get("start_time"),
        "end_time": meta.get("end_time"),
        "trigger_time": meta.get("trigger_time"),
        "duration_seconds": meta.get("duration_seconds"),
        "failed": bool(meta.get("failed", False)),
        "video_width": vc.get("width"),
        "video_height": vc.get("height"),
        "video_fps": vc.get("fps"),
        "metadata_json": json.dumps(meta, ensure_ascii=False),
    }


def flush_sessions(cur, rows: list[dict]) -> None:
    if not rows:
        return

    sql = """
        INSERT INTO hdd_sessions (
            layer, session_id, source_path, scenario,
            start_time, end_time, trigger_time, duration_seconds,
            failed, video_width, video_height, video_fps,
            metadata_json
        )
        VALUES %s
        ON CONFLICT (layer, session_id) DO UPDATE SET
            source_path       = EXCLUDED.source_path,
            scenario          = EXCLUDED.scenario,
            start_time        = EXCLUDED.start_time,
            end_time          = EXCLUDED.end_time,
            trigger_time      = EXCLUDED.trigger_time,
            duration_seconds  = EXCLUDED.duration_seconds,
            failed            = EXCLUDED.failed,
            video_width       = EXCLUDED.video_width,
            video_height      = EXCLUDED.video_height,
            video_fps         = EXCLUDED.video_fps,
            metadata_json     = EXCLUDED.metadata_json,
            synced_at         = NOW()
    """

    values = [
        (
            r["layer"],
            r["session_id"],
            r["source_path"],
            r["scenario"],
            r["start_time"],
            r["end_time"],
            r["trigger_time"],
            r["duration_seconds"],
            r["failed"],
            r["video_width"],
            r["video_height"],
            r["video_fps"],
            r["metadata_json"],
        )
        for r in rows
    ]

    execute_values(cur, sql, values, page_size=PG_BATCH_SIZE)


def flush_cameras(cur, rows: list[tuple]) -> None:
    if not rows:
        return

    sql = """
        INSERT INTO hdd_cameras (
            layer, session_id, camera_key, name, position, serial
        )
        VALUES %s
        ON CONFLICT (layer, session_id, camera_key) DO UPDATE SET
            name      = EXCLUDED.name,
            position  = EXCLUDED.position,
            serial    = EXCLUDED.serial,
            synced_at = NOW()
    """
    execute_values(cur, sql, rows, page_size=PG_BATCH_SIZE)


def flush_trackers(cur, rows: list[tuple]) -> None:
    if not rows:
        return

    sql = """
        INSERT INTO hdd_trackers (
            layer, session_id, tracker_key, serial, model
        )
        VALUES %s
        ON CONFLICT (layer, session_id, tracker_key) DO UPDATE SET
            serial    = EXCLUDED.serial,
            model     = EXCLUDED.model,
            synced_at = NOW()
    """
    execute_values(cur, sql, rows, page_size=PG_BATCH_SIZE)


def extract_camera_rows(layer: str, session_id: str, cameras: dict) -> list[tuple]:
    if not cameras:
        return []

    rows = []
    for key, cam in cameras.items():
        rows.append((
            layer,
            session_id,
            str(key),
            cam.get("name"),
            cam.get("position"),
            cam.get("serial"),
        ))
    return rows


def extract_tracker_rows(layer: str, session_id: str, trackers: dict) -> list[tuple]:
    if not trackers:
        return []

    rows = []
    for key, t in trackers.items():
        rows.append((
            layer,
            session_id,
            str(key),
            t.get("serial"),
            t.get("model"),
        ))
    return rows


# ── Helpers Superset ──────────────────────────────────────────────────────────

def register_superset() -> None:
    """
    Enregistre la base et les datasets dans Superset.
    Silencieux si Superset n'est pas prêt.
    """
    try:
        sess = requests.Session()

        resp = sess.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json={
                "username": SUPERSET_USER,
                "password": SUPERSET_PASS,
                "provider": "db",
                "refresh": True,
            },
            timeout=10,
        )
        resp.raise_for_status()

        token = resp.json().get("access_token")
        if not token:
            log.warning("Superset login échoué, registration ignorée.")
            return

        headers = {"Authorization": f"Bearer {token}"}

        csrf = sess.get(
            f"{SUPERSET_URL}/api/v1/security/csrf_token/",
            headers=headers,
            timeout=10,
        )
        csrf.raise_for_status()
        headers["X-CSRFToken"] = csrf.json().get("result", "")
        headers["Referer"] = SUPERSET_URL

        dbs = sess.get(
            f"{SUPERSET_URL}/api/v1/database/",
            headers=headers,
            timeout=10,
        ).json()

        existing = [
            d for d in dbs.get("result", [])
            if d.get("database_name") == SUPERSET_DB_NAME
        ]

        if not existing:
            r = sess.post(
                f"{SUPERSET_URL}/api/v1/database/",
                headers=headers,
                timeout=10,
                json={
                    "database_name": SUPERSET_DB_NAME,
                    "sqlalchemy_uri": SUPERSET_DB_URI,
                    "expose_in_sqllab": True,
                    "allow_run_async": False,
                    "allow_dml": False,
                },
            )
            if r.status_code in (200, 201):
                db_id = r.json()["id"]
                log.info("Base '%s' enregistrée dans Superset (id=%s)", SUPERSET_DB_NAME, db_id)
            else:
                log.warning("Erreur enregistrement DB Superset : %s", r.text)
                return
        else:
            db_id = existing[0]["id"]
            log.info("Base '%s' déjà présente dans Superset (id=%s)", SUPERSET_DB_NAME, db_id)

        for table_name in ("hdd_sessions", "hdd_cameras", "hdd_trackers", "v_sessions_full"):
            ds = sess.get(
                f"{SUPERSET_URL}/api/v1/dataset/",
                params={
                    "q": json.dumps({
                        "filters": [
                            {"col": "table_name", "opr": "eq", "val": table_name}
                        ]
                    })
                },
                headers=headers,
                timeout=10,
            ).json()

            if ds.get("count", 0) == 0:
                r = sess.post(
                    f"{SUPERSET_URL}/api/v1/dataset/",
                    headers=headers,
                    timeout=10,
                    json={
                        "database": db_id,
                        "schema": "public",
                        "table_name": table_name,
                    },
                )
                if r.status_code in (200, 201):
                    log.info("Dataset '%s' enregistré dans Superset.", table_name)
                else:
                    log.warning("Erreur dataset '%s' : %s", table_name, r.text)
            else:
                log.info("Dataset '%s' déjà présent.", table_name)

    except Exception as e:
        log.warning("Registration Superset ignorée (%s) — sera retentée plus tard.", e)


def wait_for_superset(max_wait_seconds: int = 120) -> None:
    log.info("Attente de Superset (%s)...", SUPERSET_URL)
    started = time.time()

    while time.time() - started < max_wait_seconds:
        try:
            r = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if r.status_code == 200:
                log.info("Superset prêt.")
                return
        except Exception:
            pass
        time.sleep(5)

    log.warning("Superset non prêt après %ss, on continue quand même.", max_wait_seconds)


# ── Sync principal ────────────────────────────────────────────────────────────

def ensure_database_ready() -> None:
    pg = pg_connect()
    try:
        with pg:
            with pg.cursor() as cur:
                create_schema(cur)
        log.info("Schéma PostgreSQL prêt.")
    finally:
        pg.close()


def sync_once() -> int:
    log.info("Connexion SFTP → %s", HDD_HOST)
    ssh, sftp = sftp_connect()
    pg = pg_connect()

    total_sessions = 0
    total_cameras = 0
    total_trackers = 0

    session_rows: list[dict] = []
    camera_rows: list[tuple] = []
    tracker_rows: list[tuple] = []

    try:
        cur = pg.cursor()

        for layer, base_path in LAYERS:
            metadata_files = sftp_find_metadata_files(sftp, base_path, max_depth=SFTP_MAX_DEPTH)
            log.info("[%s] %d metadata.json trouvés sous %s", layer, len(metadata_files), base_path)

            for idx, meta_path in enumerate(metadata_files, start=1):
                meta = sftp_read_json(sftp, meta_path)
                if meta is None:
                    continue

                session_dir = posixpath.dirname(meta_path)
                fallback_session_id = posixpath.basename(session_dir)
                session_row = parse_metadata(meta, layer, meta_path, fallback_session_id)

                session_id = session_row["session_id"]

                session_rows.append(session_row)
                total_sessions += 1

                cams = extract_camera_rows(layer, session_id, meta.get("cameras") or {})
                trs = extract_tracker_rows(layer, session_id, meta.get("trackers") or {})

                camera_rows.extend(cams)
                tracker_rows.extend(trs)

                total_cameras += len(cams)
                total_trackers += len(trs)

                if len(session_rows) >= PG_BATCH_SIZE:
                    flush_sessions(cur, session_rows)
                    pg.commit()
                    log.info(
                        "[%s] %d/%d sessions flushées",
                        layer, idx, len(metadata_files)
                    )
                    session_rows.clear()

                if len(camera_rows) >= PG_BATCH_SIZE:
                    flush_cameras(cur, camera_rows)
                    pg.commit()
                    camera_rows.clear()

                if len(tracker_rows) >= PG_BATCH_SIZE:
                    flush_trackers(cur, tracker_rows)
                    pg.commit()
                    tracker_rows.clear()

            if session_rows:
                flush_sessions(cur, session_rows)
                pg.commit()
                session_rows.clear()

            if camera_rows:
                flush_cameras(cur, camera_rows)
                pg.commit()
                camera_rows.clear()

            if tracker_rows:
                flush_trackers(cur, tracker_rows)
                pg.commit()
                tracker_rows.clear()

            log.info("[%s] terminé.", layer)

        cur.close()

        log.info(
            "Sync terminé : %d sessions, %d caméras, %d trackers.",
            total_sessions,
            total_cameras,
            total_trackers,
        )
        return total_sessions

    except Exception:
        pg.rollback()
        raise

    finally:
        try:
            pg.close()
        except Exception:
            pass
        try:
            sftp.close()
        except Exception:
            pass
        try:
            ssh.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync SFTP HDD → PostgreSQL")
    parser.add_argument("--watch", action="store_true", help="Boucle continue")
    parser.add_argument("--interval", type=int, default=60, help="Intervalle en secondes")
    args = parser.parse_args()

    ensure_database_ready()
    wait_for_superset(max_wait_seconds=120)
    register_superset()

    if args.watch:
        log.info("Mode watch — intervalle %ds", args.interval)
        while True:
            try:
                ensure_database_ready()
                sync_once()
                register_superset()
            except Exception as e:
                log.error("Erreur sync : %s", e)
            time.sleep(args.interval)
    else:
        sync_once()
        register_superset()


if __name__ == "__main__":
    main()
