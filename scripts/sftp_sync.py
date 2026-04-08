#!/usr/bin/env python3
"""
sftp_sync.py
============
Synchronise les metadata.json depuis le serveur SFTP HDD vers PostgreSQL.

Scanne les couches bronze / silver / gold / inbox, lit le metadata.json
de chaque session, et upserte dans :
  - hdd_sessions   : informations principales de session
  - hdd_cameras    : caméras associées
  - hdd_trackers   : trackers VIVE associés

Usage :
    python3 scripts/sftp_sync.py           # one-shot
    python3 scripts/sftp_sync.py --watch   # boucle toutes les 60 s (défaut)
    python3 scripts/sftp_sync.py --watch --interval 300
"""
import argparse
import json
import logging
import os
import time
from io import BytesIO

import paramiko
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
HDD_HOST     = os.environ.get("HDD_HOST",     "192.168.88.82")
HDD_PORT     = int(os.environ.get("HDD_PORT", "22"))
HDD_USER     = os.environ.get("HDD_USER",     "exoria")
HDD_PASSWORD = os.environ.get("HDD_PASSWORD", "Admin123456")

LAYERS = [
    ("bronze", os.environ.get("HDD_BRONZE", "/mnt/storage/bronze")),
    ("silver", os.environ.get("HDD_SILVER", "/mnt/storage/silver")),
    ("gold",   os.environ.get("HDD_GOLD",   "/mnt/storage/gold")),
    ("inbox",  os.environ.get("HDD_INBOX",  "/mnt/inbox")),
]

PG_DSN = os.environ.get(
    "ROBOTICS_DB_URI",
    "postgresql://superset:superset123@postgres:5432/robotics",
)

# ── SFTP helpers ──────────────────────────────────────────────────────────────

def sftp_connect() -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HDD_HOST, port=HDD_PORT, username=HDD_USER, password=HDD_PASSWORD)
    return ssh, ssh.open_sftp()


def sftp_listdir(sftp: paramiko.SFTPClient, path: str) -> list[str]:
    try:
        return sftp.listdir(path)
    except IOError:
        return []


def sftp_read_json(sftp: paramiko.SFTPClient, path: str) -> dict | None:
    try:
        buf = BytesIO()
        sftp.getfo(path, buf)
        return json.loads(buf.getvalue().decode())
    except Exception as e:
        log.warning("Impossible de lire %s : %s", path, e)
        return None

# ── PostgreSQL helpers ─────────────────────────────────────────────────────────

def pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(PG_DSN)


def upsert_session(cur, row: dict):
    cur.execute(
        """
        INSERT INTO hdd_sessions
            (session_id, scenario, start_time, end_time, trigger_time,
             duration_seconds, failed, video_width, video_height, video_fps,
             layer, synced_at)
        VALUES
            (%(session_id)s, %(scenario)s, %(start_time)s, %(end_time)s,
             %(trigger_time)s, %(duration_seconds)s, %(failed)s,
             %(video_width)s, %(video_height)s, %(video_fps)s,
             %(layer)s, NOW())
        ON CONFLICT (session_id) DO UPDATE SET
            scenario         = EXCLUDED.scenario,
            start_time       = EXCLUDED.start_time,
            end_time         = EXCLUDED.end_time,
            trigger_time     = EXCLUDED.trigger_time,
            duration_seconds = EXCLUDED.duration_seconds,
            failed           = EXCLUDED.failed,
            video_width      = EXCLUDED.video_width,
            video_height     = EXCLUDED.video_height,
            video_fps        = EXCLUDED.video_fps,
            layer            = EXCLUDED.layer,
            synced_at        = NOW()
        """,
        row,
    )


def upsert_cameras(cur, session_id: str, cameras: dict):
    if not cameras:
        return
    rows = [
        (session_id, key, cam.get("name"), cam.get("position"), cam.get("serial"))
        for key, cam in cameras.items()
    ]
    execute_values(
        cur,
        """
        INSERT INTO hdd_cameras (session_id, camera_key, name, position, serial)
        VALUES %s
        ON CONFLICT (session_id, camera_key) DO UPDATE SET
            name     = EXCLUDED.name,
            position = EXCLUDED.position,
            serial   = EXCLUDED.serial
        """,
        rows,
    )


def upsert_trackers(cur, session_id: str, trackers: dict):
    if not trackers:
        return
    rows = [
        (session_id, key, t.get("serial"), t.get("model"))
        for key, t in trackers.items()
    ]
    execute_values(
        cur,
        """
        INSERT INTO hdd_trackers (session_id, tracker_key, serial, model)
        VALUES %s
        ON CONFLICT (session_id, tracker_key) DO UPDATE SET
            serial = EXCLUDED.serial,
            model  = EXCLUDED.model
        """,
        rows,
    )

# ── Parsing metadata ──────────────────────────────────────────────────────────

def parse_metadata(meta: dict, layer: str) -> dict:
    vc = meta.get("video_config") or {}
    return {
        "session_id":       meta.get("session_id"),
        "scenario":         meta.get("scenario"),
        "start_time":       meta.get("start_time"),
        "end_time":         meta.get("end_time"),
        "trigger_time":     meta.get("trigger_time"),
        "duration_seconds": meta.get("duration_seconds"),
        "failed":           meta.get("failed", False),
        "video_width":      vc.get("width"),
        "video_height":     vc.get("height"),
        "video_fps":        vc.get("fps"),
        "layer":            layer,
    }

# ── Sync principal ────────────────────────────────────────────────────────────

def sync_once() -> int:
    log.info("Connexion SFTP → %s", HDD_HOST)
    ssh, sftp = sftp_connect()
    pg = pg_connect()
    cur = pg.cursor()

    total = 0
    try:
        for layer, base_path in LAYERS:
            entries = sftp_listdir(sftp, base_path)
            log.info("[%s] %d dossiers trouvés dans %s", layer, len(entries), base_path)

            for entry in entries:
                meta_path = f"{base_path}/{entry}/metadata.json"
                meta = sftp_read_json(sftp, meta_path)
                if meta is None:
                    continue

                session_id = meta.get("session_id") or entry
                row = parse_metadata(meta, layer)
                row["session_id"] = session_id

                upsert_session(cur, row)
                upsert_cameras(cur, session_id, meta.get("cameras") or {})
                upsert_trackers(cur, session_id, meta.get("trackers") or {})
                total += 1
                log.debug("  ✓ %s / %s", layer, session_id)

        pg.commit()
        log.info("Sync terminé : %d sessions chargées.", total)
    except Exception:
        pg.rollback()
        raise
    finally:
        cur.close()
        pg.close()
        sftp.close()
        ssh.close()

    return total


def main():
    parser = argparse.ArgumentParser(description="Sync SFTP HDD → PostgreSQL")
    parser.add_argument("--watch", action="store_true", help="Boucle continue")
    parser.add_argument("--interval", type=int, default=60, help="Intervalle en secondes (défaut: 60)")
    args = parser.parse_args()

    if args.watch:
        log.info("Mode watch — intervalle %ds", args.interval)
        while True:
            try:
                sync_once()
            except Exception as e:
                log.error("Erreur sync : %s", e)
            time.sleep(args.interval)
    else:
        sync_once()


if __name__ == "__main__":
    main()
