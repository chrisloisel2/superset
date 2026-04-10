#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sftp_sync.py  —  sync incrémental haute performance + mapping MongoDB -> PostgreSQL
===================================================================================

Fonctions :
  1. Scan SFTP incrémental des metadata.json
  2. Sync PostgreSQL des sessions/caméras/trackers HDD
  3. Sync MongoDB -> PostgreSQL :
       - kafka_sessions
       - session_stats
       - operators
  4. Création de vues BI pour Superset

Variables d'env :
  SFTP_WORKERS    workers SFTP   défaut: 16
  PG_BATCH_SIZE   lignes/INSERT  défaut: 500
  SFTP_MAX_DEPTH  profondeur max défaut: 8

Mongo :
  MONGO_URI       défaut: mongodb://admin:admin123@100.93.248.105/
  MONGO_DB        défaut: admin
"""

import argparse
import json
import logging
import os
import posixpath
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import datetime
from decimal import Decimal
from io import BytesIO
from stat import S_ISDIR

import paramiko
import psycopg2
import requests
from pymongo import MongoClient
from psycopg2.extras import execute_values, execute_batch, Json

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────────────

HDD_HOST     = os.environ.get("HDD_HOST",     "192.168.88.82")
HDD_PORT     = int(os.environ.get("HDD_PORT", "22"))
HDD_USER     = os.environ.get("HDD_USER",     "exoria")
HDD_PASSWORD = os.environ.get("HDD_PASSWORD", "Admin123456")

LAYERS = [
    ("inbox",  os.environ.get("HDD_INBOX",  "/mnt/inbox")),
    ("bronze", os.environ.get("HDD_BRONZE", "/mnt/storage/bronze")),
    ("silver", os.environ.get("HDD_SILVER", "/mnt/storage/silver")),
    ("gold",   os.environ.get("HDD_GOLD",   "/mnt/storage/gold")),
]

SFTP_WORKERS   = int(os.environ.get("SFTP_WORKERS",   "16"))
PG_BATCH_SIZE  = int(os.environ.get("PG_BATCH_SIZE",  "500"))
SFTP_MAX_DEPTH = int(os.environ.get("SFTP_MAX_DEPTH", "8"))

PG_DSN = os.environ.get(
    "ROBOTICS_DB_URI",
    "postgresql://superset:superset123@postgres:5432/robotics",
)

SUPERSET_URL     = os.environ.get("SUPERSET_URL",      "http://superset:8088")
SUPERSET_USER    = os.environ.get("SUPERSET_USER",     "admin")
SUPERSET_PASS    = os.environ.get("SUPERSET_PASSWORD", "Admin123456")
SUPERSET_DB_NAME = os.environ.get("SUPERSET_DB_NAME",  "HDD Robotics")
SUPERSET_DB_URI  = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://superset:superset123@postgres:5432/robotics",
)

# Mongo
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://admin:admin123@100.93.248.105/")
MONGO_DB  = os.environ.get("MONGO_DB", "admin")

COLL_KAFKA_SESSIONS = os.environ.get("COLL_KAFKA_SESSIONS", "kafka_sessions")
COLL_SESSION_STATS  = os.environ.get("COLL_SESSION_STATS", "session_stats")
COLL_OPERATORS      = os.environ.get("COLL_OPERATORS", "operators")

# ── Connexions SFTP thread-local ──────────────────────────────────────────────

_tlocal = threading.local()


def _get_sftp() -> paramiko.SFTPClient:
    if not getattr(_tlocal, "sftp", None):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            HDD_HOST, port=HDD_PORT,
            username=HDD_USER, password=HDD_PASSWORD,
            compress=False, timeout=15,
            banner_timeout=15, auth_timeout=15,
        )
        sftp = ssh.open_sftp()
        sftp.get_channel().in_window_size = 2 * 1024 * 1024
        sftp.get_channel().in_max_packet_size = 32768
        _tlocal.ssh = ssh
        _tlocal.sftp = sftp
    return _tlocal.sftp


# ── Helpers Mongo ─────────────────────────────────────────────────────────────

def mongo_connect():
    client = MongoClient(MONGO_URI)
    return client, client[MONGO_DB]


def safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def safe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def safe_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("true", "1", "yes", "y"):
            return True
        if v in ("false", "0", "no", "n"):
            return False
    return None


def parse_date(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value[:10]
    if isinstance(value, datetime):
        return value.date().isoformat()
    return None


def parse_timestamp(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


# ── Phase 1 : BFS parallèle avec collecte mtime ───────────────────────────────

def load_cache() -> dict[str, float]:
    """Charge tout hdd_scan_cache en mémoire → {path: mtime}. O(1) lookup ensuite."""
    pg = pg_connect()
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT path, mtime FROM hdd_scan_cache")
            cache = {row[0]: row[1] for row in cur.fetchall()}
        log.info("Cache chargé : %d entrées", len(cache))
        return cache
    finally:
        pg.close()


def _scan_and_read(
    path: str,
    depth: int,
    layer: str,
    cache: dict[str, float],
) -> tuple[list[tuple[str, int, str]], list[tuple[str, float, dict]]]:
    """
    Worker unifié BFS + lecture immédiate.
    - Liste le répertoire
    - Pour chaque metadata.json : vérifie mtime vs cache en mémoire
      → si nouveau/modifié : lit et parse immédiatement
    Retourne :
      subdirs  : [(path, depth, layer), ...]
      sessions : [(path, mtime, meta_dict), ...]
    """
    if depth > SFTP_MAX_DEPTH:
        return [], []
    sftp = _get_sftp()
    try:
        entries = sftp.listdir_attr(path)
    except Exception as e:
        log.warning("listdir %s : %s", path, e)
        return [], []

    subdirs:  list[tuple[str, int, str]]     = []
    sessions: list[tuple[str, float, dict]]  = []

    for entry in entries:
        full = posixpath.join(path, entry.filename)
        try:
            if S_ISDIR(entry.st_mode):
                subdirs.append((full, depth + 1, layer))
            elif entry.filename == "metadata.json":
                mtime = float(entry.st_mtime or 0)
                cached = cache.get(full)
                if cached is not None and abs(cached - mtime) <= 1:
                    continue  # déjà traité, même mtime → skip
                try:
                    buf = BytesIO()
                    sftp.getfo(full, buf)
                    meta = json.loads(buf.getvalue().decode("utf-8"))
                    sessions.append((full, mtime, meta))
                except FileNotFoundError:
                    log.debug("Disparu : %s", full)
                except Exception as e:
                    log.warning("Lecture %s : %s", full, e)
        except Exception:
            pass
    return subdirs, sessions


def _hdd_sync_pipeline(flush_size: int = 200) -> int:
    """
    Pipeline unifié : BFS + lecture + INSERT en une seule boucle.
    Les données sont flushées en base tous les `flush_size` sessions
    PENDANT le scan, sans attendre la fin du BFS.
    """
    cache = load_cache()

    session_buf: list[dict]              = []
    camera_buf:  list[tuple]             = []
    tracker_buf: list[tuple]             = []
    cache_buf:   list[tuple[str, float]] = []

    total_sessions = 0
    dirs_done      = 0
    last_log       = time.perf_counter()
    pending: dict  = {}

    pg  = pg_connect()
    cur = pg.cursor()

    def _flush(force: bool = False) -> None:
        nonlocal total_sessions
        if not force and len(session_buf) < flush_size:
            return
        if not session_buf:
            return
        try:
            flush_sessions(cur, session_buf)
            pg.commit()
            flush_cameras(cur, camera_buf)
            pg.commit()
            flush_trackers(cur, tracker_buf)
            pg.commit()
            # Mise à jour cache dans la même transaction
            execute_values(cur, """
                INSERT INTO hdd_scan_cache (path, mtime, synced_at) VALUES %s
                ON CONFLICT (path) DO UPDATE SET mtime=EXCLUDED.mtime, synced_at=NOW()
            """, cache_buf, page_size=PG_BATCH_SIZE)
            pg.commit()
            total_sessions += len(session_buf)
            log.info("Flush +%d sessions (total %d) | dirs: %d | en file: %d",
                     len(session_buf), total_sessions, dirs_done, len(pending))
            session_buf.clear(); camera_buf.clear()
            tracker_buf.clear(); cache_buf.clear()
        except Exception as e:
            pg.rollback()
            log.error("Erreur flush : %s", e)
            raise

    try:
        with ThreadPoolExecutor(max_workers=SFTP_WORKERS, thread_name_prefix="sftp") as ex:
            for layer, base in LAYERS:
                f = ex.submit(_scan_and_read, base, 0, layer, cache)
                pending[f] = (layer, base)
            log.info("Pipeline BFS+lecture démarré — %d layers, %d workers, flush/%d",
                     len(LAYERS), SFTP_WORKERS, flush_size)

            while pending:
                done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                new: dict = {}
                for f in done:
                    layer, path = pending.pop(f)
                    dirs_done += 1
                    try:
                        subdirs, sessions = f.result()
                        for sp, d, sl in subdirs:
                            nf = ex.submit(_scan_and_read, sp, d, sl, cache)
                            new[nf] = (sl, sp)
                        for spath, mtime, meta in sessions:
                            fallback = posixpath.basename(posixpath.dirname(spath))
                            row = parse_metadata(meta, layer, spath, fallback)
                            sid = row["session_id"]
                            session_buf.append(row)
                            camera_buf.extend([
                                (layer, sid, str(k), c.get("name"), c.get("position"), c.get("serial"))
                                for k, c in (meta.get("cameras") or {}).items()
                            ])
                            tracker_buf.extend([
                                (layer, sid, str(k), t.get("serial"), t.get("model"))
                                for k, t in (meta.get("trackers") or {}).items()
                            ])
                            cache_buf.append((spath, mtime))
                    except Exception as e:
                        log.warning("Worker %s : %s", path, e)
                pending.update(new)

                if time.perf_counter() - last_log >= 30:
                    log.info("  … dirs: %d | buf: %d sessions | en file: %d",
                             dirs_done, len(session_buf), len(pending))
                    last_log = time.perf_counter()

                _flush(force=False)

        _flush(force=True)
        cur.close()
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()

    log.info("HDD pipeline terminé — %d sessions insérées", total_sessions)
    return total_sessions


def _scan_dir(path: str, depth: int) -> tuple[list[tuple[str, int]], list[tuple[str, float]]]:
    if depth > SFTP_MAX_DEPTH:
        return [], []
    try:
        entries = _get_sftp().listdir_attr(path)
    except Exception as e:
        log.warning("listdir %s : %s", path, e)
        return [], []

    subdirs = []
    metas = []

    for entry in entries:
        full = posixpath.join(path, entry.filename)
        try:
            if S_ISDIR(entry.st_mode):
                subdirs.append((full, depth + 1))
            elif entry.filename == "metadata.json":
                metas.append((full, float(entry.st_mtime or 0)))
        except Exception:
            pass
    return subdirs, metas


def find_all_metadata_parallel() -> dict[str, list[tuple[str, float]]]:
    layer_results = {l: [] for l, _ in LAYERS}
    futures = {}
    dirs_scanned = 0

    with ThreadPoolExecutor(max_workers=SFTP_WORKERS, thread_name_prefix="sftp-scan") as ex:
        for layer, base in LAYERS:
            f = ex.submit(_scan_dir, base, 0)
            futures[f] = (layer, base)
        log.info("Scan BFS démarré — %d layers, %d workers", len(LAYERS), SFTP_WORKERS)

        while futures:
            done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
            new_futures = {}
            for f in done:
                layer, path = futures.pop(f)
                dirs_scanned += 1
                try:
                    subdirs, metas = f.result()
                    layer_results[layer].extend(metas)
                    for subdir, d in subdirs:
                        nf = ex.submit(_scan_dir, subdir, d)
                        new_futures[nf] = (layer, subdir)
                    if dirs_scanned % 100 == 0:
                        total = sum(len(v) for v in layer_results.values())
                        log.info("  scan: %d dossiers, %d metadata.json, %d en file",
                                 dirs_scanned, total, len(futures) + len(new_futures))
                except Exception as e:
                    log.warning("Erreur scan %s : %s", path, e)
            futures.update(new_futures)

    total = sum(len(v) for v in layer_results.values())
    log.info("Scan terminé — %d metadata.json trouvés", total)
    for layer, items in layer_results.items():
        log.info("  [%s] %d fichiers", layer, len(items))
    return layer_results


# ── Filtre incrémental ────────────────────────────────────────────────────────

def filter_new_or_changed(
    layer_results: dict[str, list[tuple[str, float]]]
) -> dict[str, list[tuple[str, float]]]:
    all_paths = [path for paths in layer_results.values() for path, _ in paths]
    if not all_paths:
        return {l: [] for l in layer_results}

    pg = pg_connect()
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT path, mtime FROM hdd_scan_cache WHERE path = ANY(%s)",
                (all_paths,)
            )
            cached = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        pg.close()

    filtered = {}
    total_new = 0
    total_skip = 0

    for layer, items in layer_results.items():
        new_items = []
        for path, mtime in items:
            cached_mtime = cached.get(path)
            if cached_mtime is None or abs(cached_mtime - mtime) > 1:
                new_items.append((path, mtime))
                total_new += 1
            else:
                total_skip += 1
        filtered[layer] = new_items

    log.info("Filtre incrémental : %d nouveaux/modifiés, %d déjà en cache (ignorés)",
             total_new, total_skip)
    return filtered


def update_scan_cache(processed: list[tuple[str, float]]) -> None:
    if not processed:
        return
    pg = pg_connect()
    try:
        with pg.cursor() as cur:
            execute_values(cur, """
                INSERT INTO hdd_scan_cache (path, mtime, synced_at)
                VALUES %s
                ON CONFLICT (path) DO UPDATE SET
                    mtime     = EXCLUDED.mtime,
                    synced_at = NOW()
            """, [(path, mtime) for path, mtime in processed], page_size=PG_BATCH_SIZE)
        pg.commit()
        log.info("Cache mis à jour : %d entrées", len(processed))
    finally:
        pg.close()


# ── Phase 2 : lecture parallèle ───────────────────────────────────────────────

def _read_meta(layer: str, path: str, mtime: float):
    try:
        buf = BytesIO()
        _get_sftp().getfo(path, buf)
        return layer, path, mtime, json.loads(buf.getvalue().decode("utf-8"))
    except FileNotFoundError:
        log.debug("Disparu : %s", path)
        return None
    except Exception as e:
        log.warning("Lecture %s : %s", path, e)
        return None


def read_and_flush_pipeline(
    filtered: dict[str, list[tuple[str, float]]],
    flush_size: int = 200,
) -> tuple[int, list[tuple[str, float]]]:
    all_items = [
        (layer, path, mtime)
        for layer, items in filtered.items()
        for path, mtime in items
    ]
    total = len(all_items)
    if total == 0:
        return 0, []

    t0 = time.perf_counter()
    log.info("Pipeline lecture+INSERT — %d fichiers, workers: %d, flush tous les %d",
             total, SFTP_WORKERS, flush_size)

    session_buf = []
    camera_buf = []
    tracker_buf = []
    cache_buf = []

    total_sessions = 0
    total_cameras = 0
    total_trackers = 0
    files_read = 0

    pg = pg_connect()
    cur = pg.cursor()

    def flush_buffers(force: bool = False) -> None:
        nonlocal total_sessions, total_cameras, total_trackers
        if not force and len(session_buf) < flush_size:
            return

        try:
            flush_sessions(cur, session_buf)
            pg.commit()
            flush_cameras(cur, camera_buf)
            pg.commit()
            flush_trackers(cur, tracker_buf)
            pg.commit()

            update_scan_cache(cache_buf)

            total_sessions += len(session_buf)
            total_cameras += len(camera_buf)
            total_trackers += len(tracker_buf)

            log.info("  Flush — %d sessions insérées (total %d), lu %d/%d",
                     len(session_buf), total_sessions, files_read, total)

            session_buf.clear()
            camera_buf.clear()
            tracker_buf.clear()
            cache_buf.clear()

        except Exception as e:
            pg.rollback()
            log.error("Erreur flush : %s", e)
            raise

    try:
        with ThreadPoolExecutor(max_workers=SFTP_WORKERS, thread_name_prefix="sftp-read") as ex:
            futures = {
                ex.submit(_read_meta, layer, path, mtime): (path, mtime)
                for layer, path, mtime in all_items
            }
            for f in as_completed(futures):
                files_read += 1
                result = f.result()
                if result is None:
                    continue

                layer, path, mtime, meta = result
                fallback = posixpath.basename(posixpath.dirname(path))
                row = parse_metadata(meta, layer, path, fallback)
                session_id = row["session_id"]

                session_buf.append(row)
                camera_buf.extend([
                    (layer, session_id, str(k), c.get("name"), c.get("position"), c.get("serial"))
                    for k, c in (meta.get("cameras") or {}).items()
                ])
                tracker_buf.extend([
                    (layer, session_id, str(k), t.get("serial"), t.get("model"))
                    for k, t in (meta.get("trackers") or {}).items()
                ])
                cache_buf.append((path, mtime))

                flush_buffers(force=False)

        flush_buffers(force=True)
        cur.close()

    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()

    elapsed = time.perf_counter() - t0
    rate = total / elapsed if elapsed > 0 else 0
    log.info("Pipeline terminé en %.1fs — %d sessions, %d caméras, %d trackers (%.0f f/s)",
             elapsed, total_sessions, total_cameras, total_trackers, rate)

    return total_sessions, []


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def pg_connect():
    return psycopg2.connect(PG_DSN)


def create_schema(cur) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS bi")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdd_sessions (
            id               BIGSERIAL PRIMARY KEY,
            layer            TEXT NOT NULL,
            session_id       TEXT NOT NULL,
            source_path      TEXT NOT NULL,
            scenario         TEXT,
            start_time       TIMESTAMPTZ,
            end_time         TIMESTAMPTZ,
            trigger_time     TIMESTAMPTZ,
            duration_seconds DOUBLE PRECISION,
            failed           BOOLEAN DEFAULT FALSE,
            video_width      INTEGER,
            video_height     INTEGER,
            video_fps        DOUBLE PRECISION,
            synced_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            metadata_json    JSONB,
            CONSTRAINT uq_hdd_sessions UNIQUE (layer, session_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdd_cameras (
            id          BIGSERIAL PRIMARY KEY,
            layer       TEXT NOT NULL,
            session_id  TEXT NOT NULL,
            camera_key  TEXT NOT NULL,
            name        TEXT,
            position    TEXT,
            serial      TEXT,
            synced_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_hdd_cameras UNIQUE (layer, session_id, camera_key)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdd_trackers (
            id          BIGSERIAL PRIMARY KEY,
            layer       TEXT NOT NULL,
            session_id  TEXT NOT NULL,
            tracker_key TEXT NOT NULL,
            serial      TEXT,
            model       TEXT,
            synced_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_hdd_trackers UNIQUE (layer, session_id, tracker_key)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdd_scan_cache (
            path      TEXT PRIMARY KEY,
            mtime     DOUBLE PRECISION NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # Tables Mongo staging
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.mongo_kafka_sessions (
            session_id TEXT PRIMARY KEY,
            mongo_id TEXT,
            station_id TEXT,
            operator_code TEXT,
            scenario TEXT,
            ts_stop DOUBLE PRECISION,
            duration_s DOUBLE PRECISION,
            failed BOOLEAN,
            upload_status TEXT,
            size_gb DOUBLE PRECISION,
            session_date DATE,
            hour INTEGER,
            month TEXT,
            raw_doc JSONB,
            loaded_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.mongo_session_stats (
            session_id TEXT PRIMARY KEY,
            mongo_id TEXT,
            station_id TEXT,
            operator_code TEXT,
            scenario TEXT,
            duration_s DOUBLE PRECISION,
            size_gb DOUBLE PRECISION,
            failed BOOLEAN,
            upload_success BOOLEAN,
            session_date DATE,
            hour INTEGER,
            ingested_at TIMESTAMPTZ,
            raw_doc JSONB,
            loaded_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.mongo_operators (
            operator_id TEXT PRIMARY KEY,
            employee_code TEXT,
            full_name TEXT,
            username TEXT,
            password TEXT,
            role TEXT,
            site_id TEXT,
            rig_id TEXT,
            status TEXT,
            hourly_cost NUMERIC(12,4),
            currency TEXT,
            raw_doc JSONB,
            loaded_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_layer ON hdd_sessions(layer)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_session_id ON hdd_sessions(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_start_time ON hdd_sessions(start_time)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_meta_gin ON hdd_sessions USING GIN (metadata_json)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_cameras_layer_sess ON hdd_cameras(layer, session_id)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_trackers_layer_sess ON hdd_trackers(layer, session_id)",
        "CREATE INDEX IF NOT EXISTS idx_mongo_kafka_sessions_date ON staging.mongo_kafka_sessions(session_date)",
        "CREATE INDEX IF NOT EXISTS idx_mongo_kafka_sessions_operator ON staging.mongo_kafka_sessions(operator_code)",
        "CREATE INDEX IF NOT EXISTS idx_mongo_kafka_sessions_station ON staging.mongo_kafka_sessions(station_id)",
        "CREATE INDEX IF NOT EXISTS idx_mongo_session_stats_date ON staging.mongo_session_stats(session_date)",
        "CREATE INDEX IF NOT EXISTS idx_mongo_session_stats_operator ON staging.mongo_session_stats(operator_code)",
        "CREATE INDEX IF NOT EXISTS idx_mongo_session_stats_station ON staging.mongo_session_stats(station_id)",
        "CREATE INDEX IF NOT EXISTS idx_mongo_operators_username ON staging.mongo_operators(username)",
    ]:
        cur.execute(ddl)

    # Vue HDD existante
    cur.execute("""
        CREATE OR REPLACE VIEW v_sessions_full AS
        SELECT
            s.id, s.layer, s.session_id, s.source_path, s.scenario,
            s.start_time, s.end_time, s.trigger_time,
            s.duration_seconds, s.failed,
            s.video_width, s.video_height, s.video_fps,
            s.synced_at, s.metadata_json,
            COALESCE(c.cameras_count,  0) AS cameras_count,
            COALESCE(t.trackers_count, 0) AS trackers_count
        FROM hdd_sessions s
        LEFT JOIN (
            SELECT layer, session_id, COUNT(*) AS cameras_count
            FROM hdd_cameras GROUP BY layer, session_id
        ) c ON c.layer = s.layer AND c.session_id = s.session_id
        LEFT JOIN (
            SELECT layer, session_id, COUNT(*) AS trackers_count
            FROM hdd_trackers GROUP BY layer, session_id
        ) t ON t.layer = s.layer AND t.session_id = s.session_id
    """)

    # Vues BI Mongo + HDD
    cur.execute("""
        CREATE OR REPLACE VIEW bi.dim_operators AS
        SELECT
            operator_id,
            employee_code,
            full_name,
            username,
            role,
            site_id,
            rig_id,
            status,
            hourly_cost,
            currency
        FROM staging.mongo_operators
    """)

    cur.execute("""
        CREATE OR REPLACE VIEW bi.fact_capture_sessions AS
        WITH mongo_union AS (
            SELECT
                ks.session_id,
                ks.station_id,
                ks.operator_code AS operator,
                ks.scenario,
                ks.session_date,
                ks.hour,
                CASE
                    WHEN ks.hour BETWEEN 6 AND 13 THEN 'shift_1'
                    WHEN ks.hour BETWEEN 14 AND 21 THEN 'shift_2'
                    ELSE 'shift_3'
                END AS shift,
                ks.duration_s,
                ks.duration_s / 3600.0 AS raw_hours,
                ks.failed,
                NULL::BOOLEAN AS upload_success,
                ks.upload_status,
                ks.size_gb,
                ks.month,
                NULL::TIMESTAMPTZ AS ingested_at,
                'kafka_sessions'::TEXT AS source
            FROM staging.mongo_kafka_sessions ks

            UNION ALL

            SELECT
                ss.session_id,
                ss.station_id,
                ss.operator_code AS operator,
                ss.scenario,
                ss.session_date,
                ss.hour,
                CASE
                    WHEN ss.hour BETWEEN 6 AND 13 THEN 'shift_1'
                    WHEN ss.hour BETWEEN 14 AND 21 THEN 'shift_2'
                    ELSE 'shift_3'
                END AS shift,
                ss.duration_s,
                ss.duration_s / 3600.0 AS raw_hours,
                ss.failed,
                ss.upload_success,
                CASE
                    WHEN ss.upload_success IS TRUE THEN 'success'
                    WHEN ss.upload_success IS FALSE THEN 'failed'
                    ELSE NULL
                END AS upload_status,
                ss.size_gb,
                TO_CHAR(ss.session_date, 'YYYY-MM') AS month,
                ss.ingested_at,
                'session_stats'::TEXT AS source
            FROM staging.mongo_session_stats ss
        ),
        dedup AS (
            SELECT *
            FROM (
                SELECT
                    m.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.session_id
                        ORDER BY
                            CASE WHEN m.source = 'session_stats' THEN 1 ELSE 2 END,
                            m.ingested_at DESC NULLS LAST
                    ) AS rn
                FROM mongo_union m
            ) x
            WHERE rn = 1
        )
        SELECT
            d.session_id,
            d.station_id,
            d.operator,
            d.scenario,
            d.session_date,
            d.hour,
            d.shift,
            d.duration_s,
            d.raw_hours,
            d.failed,
            d.upload_success,
            d.upload_status,
            d.size_gb,
            d.month,
            d.ingested_at,
            d.source,
            o.operator_id,
            o.employee_code,
            o.full_name,
            o.role,
            o.site_id,
            o.rig_id,
            o.status AS operator_status,
            o.hourly_cost,
            o.currency,
            v.start_time,
            v.end_time,
            v.trigger_time,
            v.synced_at,
            v.duration_seconds AS hdd_duration_seconds,
            v.layer,
            v.source_path,
            v.video_width,
            v.video_height,
            v.video_fps,
            v.metadata_json,
            v.cameras_count,
            v.trackers_count,
            CASE
                WHEN v.end_time IS NOT NULL AND v.synced_at IS NOT NULL
                THEN EXTRACT(EPOCH FROM (v.synced_at - v.end_time)) / 60.0
                ELSE NULL
            END AS capture_to_sync_latency_min
        FROM dedup d
        LEFT JOIN bi.dim_operators o
            ON UPPER(d.operator) = UPPER(o.username)
        LEFT JOIN v_sessions_full v
            ON d.session_id = v.session_id
    """)

    cur.execute("""
        CREATE OR REPLACE VIEW bi.operator_activity_daily AS
        SELECT
            session_date,
            operator,
            full_name,
            site_id,
            rig_id,
            hourly_cost,
            COUNT(*) AS sessions_count,
            SUM(raw_hours) AS raw_hours,
            SUM(CASE WHEN failed = FALSE THEN raw_hours ELSE 0 END) AS accepted_hours_proxy,
            AVG(CASE WHEN failed = TRUE THEN 1.0 ELSE 0.0 END) AS failed_rate,
            AVG(
                CASE
                    WHEN upload_success = TRUE OR upload_status = 'success' THEN 1.0
                    ELSE 0.0
                END
            ) AS upload_success_rate,
            SUM(raw_hours * COALESCE(hourly_cost, 0)) AS labor_cost_proxy
        FROM bi.fact_capture_sessions
        GROUP BY session_date, operator, full_name, site_id, rig_id, hourly_cost
    """)

    cur.execute("""
        CREATE OR REPLACE VIEW bi.rig_activity_daily AS
        SELECT
            session_date,
            station_id,
            COUNT(*) AS sessions_count,
            SUM(raw_hours) AS raw_hours,
            SUM(CASE WHEN failed = FALSE THEN raw_hours ELSE 0 END) AS accepted_hours_proxy,
            AVG(CASE WHEN failed = TRUE THEN 1.0 ELSE 0.0 END) AS failed_rate,
            AVG(
                CASE
                    WHEN upload_success = TRUE OR upload_status = 'success' THEN 1.0
                    ELSE 0.0
                END
            ) AS upload_success_rate
        FROM bi.fact_capture_sessions
        GROUP BY session_date, station_id
    """)


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_metadata(meta: dict, layer: str, source_path: str, fallback: str) -> dict:
    vc = meta.get("video_config") or {}
    return {
        "layer":            layer,
        "session_id":       meta.get("session_id") or fallback,
        "source_path":      source_path,
        "scenario":         meta.get("scenario"),
        "start_time":       meta.get("start_time"),
        "end_time":         meta.get("end_time"),
        "trigger_time":     meta.get("trigger_time"),
        "duration_seconds": meta.get("duration_seconds"),
        "failed":           bool(meta.get("failed", False)),
        "video_width":      vc.get("width"),
        "video_height":     vc.get("height"),
        "video_fps":        vc.get("fps"),
        "metadata_json":    json.dumps(meta, ensure_ascii=False),
    }


# ── INSERT batch HDD ──────────────────────────────────────────────────────────

def flush_sessions(cur, rows: list[dict]) -> None:
    if not rows:
        return
    deduped = {(r["layer"], r["session_id"]): r for r in rows}
    execute_values(cur, """
        INSERT INTO hdd_sessions (
            layer, session_id, source_path, scenario,
            start_time, end_time, trigger_time, duration_seconds,
            failed, video_width, video_height, video_fps, metadata_json
        )
        VALUES %s
        ON CONFLICT (layer, session_id) DO UPDATE SET
            source_path      = EXCLUDED.source_path,
            scenario         = EXCLUDED.scenario,
            start_time       = EXCLUDED.start_time,
            end_time         = EXCLUDED.end_time,
            trigger_time     = EXCLUDED.trigger_time,
            duration_seconds = EXCLUDED.duration_seconds,
            failed           = EXCLUDED.failed,
            video_width      = EXCLUDED.video_width,
            video_height     = EXCLUDED.video_height,
            video_fps        = EXCLUDED.video_fps,
            metadata_json    = EXCLUDED.metadata_json,
            synced_at        = NOW()
    """, [(
        r["layer"], r["session_id"], r["source_path"], r["scenario"],
        r["start_time"], r["end_time"], r["trigger_time"], r["duration_seconds"],
        r["failed"], r["video_width"], r["video_height"], r["video_fps"],
        r["metadata_json"],
    ) for r in deduped.values()], page_size=PG_BATCH_SIZE)


def flush_cameras(cur, rows: list[tuple]) -> None:
    if not rows:
        return
    deduped = {(r[0], r[1], r[2]): r for r in rows}
    execute_values(cur, """
        INSERT INTO hdd_cameras (layer, session_id, camera_key, name, position, serial)
        VALUES %s
        ON CONFLICT (layer, session_id, camera_key) DO UPDATE SET
            name      = EXCLUDED.name,
            position  = EXCLUDED.position,
            serial    = EXCLUDED.serial,
            synced_at = NOW()
    """, list(deduped.values()), page_size=PG_BATCH_SIZE)


def flush_trackers(cur, rows: list[tuple]) -> None:
    if not rows:
        return
    deduped = {(r[0], r[1], r[2]): r for r in rows}
    execute_values(cur, """
        INSERT INTO hdd_trackers (layer, session_id, tracker_key, serial, model)
        VALUES %s
        ON CONFLICT (layer, session_id, tracker_key) DO UPDATE SET
            serial    = EXCLUDED.serial,
            model     = EXCLUDED.model,
            synced_at = NOW()
    """, list(deduped.values()), page_size=PG_BATCH_SIZE)


# ── Mongo extract/load ────────────────────────────────────────────────────────

def extract_kafka_sessions(db):
    docs = list(db[COLL_KAFKA_SESSIONS].find({}))
    log.info("Mongo %s: %d documents", COLL_KAFKA_SESSIONS, len(docs))
    out = []
    for d in docs:
        out.append({
            "session_id": d.get("session_id") or str(d.get("_id")),
            "mongo_id": str(d.get("_id")),
            "station_id": d.get("station_id"),
            "operator_code": d.get("operator"),
            "scenario": d.get("scenario"),
            "ts_stop": safe_float(d.get("ts_stop")),
            "duration_s": safe_float(d.get("duration_s")),
            "failed": safe_bool(d.get("failed")),
            "upload_status": d.get("upload_status"),
            "size_gb": safe_float(d.get("size_gb")),
            "session_date": parse_date(d.get("date")),
            "hour": safe_int(d.get("hour")),
            "month": d.get("month"),
            "raw_doc": d,
        })
    return out


def extract_session_stats(db):
    docs = list(db[COLL_SESSION_STATS].find({}))
    log.info("Mongo %s: %d documents", COLL_SESSION_STATS, len(docs))
    out = []
    for d in docs:
        out.append({
            "session_id": d.get("session_id") or str(d.get("_id")),
            "mongo_id": str(d.get("_id")),
            "station_id": d.get("station_id"),
            "operator_code": d.get("operator"),
            "scenario": d.get("scenario"),
            "duration_s": safe_float(d.get("duration_s")),
            "size_gb": safe_float(d.get("size_gb")),
            "failed": safe_bool(d.get("failed")),
            "upload_success": safe_bool(d.get("upload_success")),
            "session_date": parse_date(d.get("date")),
            "hour": safe_int(d.get("hour")),
            "ingested_at": parse_timestamp(d.get("ingested_at")),
            "raw_doc": d,
        })
    return out


def extract_operators(db):
    docs = list(db[COLL_OPERATORS].find({}))
    log.info("Mongo %s: %d documents", COLL_OPERATORS, len(docs))
    out = []
    for d in docs:
        cost_profile = d.get("cost_profile") or {}
        out.append({
            "operator_id": d.get("_id"),
            "employee_code": d.get("employee_code"),
            "full_name": d.get("full_name"),
            "username": d.get("username"),
            "password": d.get("password"),
            "role": d.get("role"),
            "site_id": d.get("site_id"),
            "rig_id": d.get("rig_id"),
            "status": d.get("status"),
            "hourly_cost": Decimal(str(cost_profile.get("hourly_cost"))) if cost_profile.get("hourly_cost") is not None else None,
            "currency": cost_profile.get("currency"),
            "raw_doc": d,
        })
    return out


def upsert_mongo_kafka_sessions(cur, rows):
    if not rows:
        return
    sql = """
        INSERT INTO staging.mongo_kafka_sessions (
            session_id, mongo_id, station_id, operator_code, scenario,
            ts_stop, duration_s, failed, upload_status, size_gb,
            session_date, hour, month, raw_doc
        ) VALUES (
            %(session_id)s, %(mongo_id)s, %(station_id)s, %(operator_code)s, %(scenario)s,
            %(ts_stop)s, %(duration_s)s, %(failed)s, %(upload_status)s, %(size_gb)s,
            %(session_date)s, %(hour)s, %(month)s, %(raw_doc)s
        )
        ON CONFLICT (session_id) DO UPDATE SET
            mongo_id = EXCLUDED.mongo_id,
            station_id = EXCLUDED.station_id,
            operator_code = EXCLUDED.operator_code,
            scenario = EXCLUDED.scenario,
            ts_stop = EXCLUDED.ts_stop,
            duration_s = EXCLUDED.duration_s,
            failed = EXCLUDED.failed,
            upload_status = EXCLUDED.upload_status,
            size_gb = EXCLUDED.size_gb,
            session_date = EXCLUDED.session_date,
            hour = EXCLUDED.hour,
            month = EXCLUDED.month,
            raw_doc = EXCLUDED.raw_doc,
            loaded_at = NOW()
    """
    payload = []
    for row in rows:
        r = dict(row)
        r["raw_doc"] = Json(r["raw_doc"])
        payload.append(r)
    execute_batch(cur, sql, payload, page_size=PG_BATCH_SIZE)


def upsert_mongo_session_stats(cur, rows):
    if not rows:
        return
    sql = """
        INSERT INTO staging.mongo_session_stats (
            session_id, mongo_id, station_id, operator_code, scenario,
            duration_s, size_gb, failed, upload_success,
            session_date, hour, ingested_at, raw_doc
        ) VALUES (
            %(session_id)s, %(mongo_id)s, %(station_id)s, %(operator_code)s, %(scenario)s,
            %(duration_s)s, %(size_gb)s, %(failed)s, %(upload_success)s,
            %(session_date)s, %(hour)s, %(ingested_at)s, %(raw_doc)s
        )
        ON CONFLICT (session_id) DO UPDATE SET
            mongo_id = EXCLUDED.mongo_id,
            station_id = EXCLUDED.station_id,
            operator_code = EXCLUDED.operator_code,
            scenario = EXCLUDED.scenario,
            duration_s = EXCLUDED.duration_s,
            size_gb = EXCLUDED.size_gb,
            failed = EXCLUDED.failed,
            upload_success = EXCLUDED.upload_success,
            session_date = EXCLUDED.session_date,
            hour = EXCLUDED.hour,
            ingested_at = EXCLUDED.ingested_at,
            raw_doc = EXCLUDED.raw_doc,
            loaded_at = NOW()
    """
    payload = []
    for row in rows:
        r = dict(row)
        r["raw_doc"] = Json(r["raw_doc"])
        payload.append(r)
    execute_batch(cur, sql, payload, page_size=PG_BATCH_SIZE)


def upsert_mongo_operators(cur, rows):
    if not rows:
        return
    sql = """
        INSERT INTO staging.mongo_operators (
            operator_id, employee_code, full_name, username, password, role,
            site_id, rig_id, status, hourly_cost, currency, raw_doc
        ) VALUES (
            %(operator_id)s, %(employee_code)s, %(full_name)s, %(username)s, %(password)s, %(role)s,
            %(site_id)s, %(rig_id)s, %(status)s, %(hourly_cost)s, %(currency)s, %(raw_doc)s
        )
        ON CONFLICT (operator_id) DO UPDATE SET
            employee_code = EXCLUDED.employee_code,
            full_name = EXCLUDED.full_name,
            username = EXCLUDED.username,
            password = EXCLUDED.password,
            role = EXCLUDED.role,
            site_id = EXCLUDED.site_id,
            rig_id = EXCLUDED.rig_id,
            status = EXCLUDED.status,
            hourly_cost = EXCLUDED.hourly_cost,
            currency = EXCLUDED.currency,
            raw_doc = EXCLUDED.raw_doc,
            loaded_at = NOW()
    """
    payload = []
    for row in rows:
        r = dict(row)
        r["raw_doc"] = Json(r["raw_doc"])
        payload.append(r)
    execute_batch(cur, sql, payload, page_size=PG_BATCH_SIZE)


def sync_mongo_once() -> None:
    log.info("Début sync MongoDB -> PostgreSQL")
    client, db = mongo_connect()
    pg = pg_connect()
    try:
        kafka_sessions = extract_kafka_sessions(db)
        session_stats = extract_session_stats(db)
        operators = extract_operators(db)

        with pg.cursor() as cur:
            upsert_mongo_operators(cur, operators)
            upsert_mongo_kafka_sessions(cur, kafka_sessions)
            upsert_mongo_session_stats(cur, session_stats)

        pg.commit()
        log.info(
            "Sync Mongo terminé — operators=%d, kafka_sessions=%d, session_stats=%d",
            len(operators), len(kafka_sessions), len(session_stats)
        )
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()
        client.close()


# ── Sync principal ────────────────────────────────────────────────────────────

def sync_once() -> int:
    t0 = time.perf_counter()

    _hdd_sync_pipeline(flush_size=200)

    # Sync Mongo à chaque exécution
    sync_mongo_once()

    log.info("Sync complet en %.1fs total", time.perf_counter() - t0)
    return 1


# ── Superset ──────────────────────────────────────────────────────────────────

def register_superset() -> None:
    try:
        sess = requests.Session()
        resp = sess.post(f"{SUPERSET_URL}/api/v1/security/login", json={
            "username": SUPERSET_USER,
            "password": SUPERSET_PASS,
            "provider": "db",
            "refresh": True,
        }, timeout=10)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            return

        headers = {
            "Authorization": f"Bearer {token}",
            "Referer": SUPERSET_URL,
        }
        csrf = sess.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers, timeout=10)
        csrf.raise_for_status()
        headers["X-CSRFToken"] = csrf.json().get("result", "")

        dbs = sess.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers, timeout=10).json()
        existing = [d for d in dbs.get("result", []) if d.get("database_name") == SUPERSET_DB_NAME]

        if not existing:
            r = sess.post(f"{SUPERSET_URL}/api/v1/database/", headers=headers, timeout=10, json={
                "database_name": SUPERSET_DB_NAME,
                "sqlalchemy_uri": SUPERSET_DB_URI,
                "expose_in_sqllab": True,
                "allow_run_async": True,
                "allow_dml": False,
            })
            db_id = r.json().get("id") if r.status_code in (200, 201) else None
            if not db_id:
                log.warning("Erreur enregistrement DB Superset : %s", r.text)
                return
            log.info("Base '%s' enregistrée (id=%s)", SUPERSET_DB_NAME, db_id)
        else:
            db_id = existing[0]["id"]
            log.info("Base '%s' déjà présente (id=%s)", SUPERSET_DB_NAME, db_id)

        for schema_name, table_name in [
            ("public", "hdd_sessions"),
            ("public", "hdd_cameras"),
            ("public", "hdd_trackers"),
            ("public", "v_sessions_full"),
            ("staging", "mongo_kafka_sessions"),
            ("staging", "mongo_session_stats"),
            ("staging", "mongo_operators"),
            ("bi", "dim_operators"),
            ("bi", "fact_capture_sessions"),
            ("bi", "operator_activity_daily"),
            ("bi", "rig_activity_daily"),
        ]:
            ds = sess.get(
                f"{SUPERSET_URL}/api/v1/dataset/",
                headers=headers,
                timeout=10,
                params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "val": table_name}]})},
            ).json()

            if ds.get("count", 0) == 0:
                r = sess.post(
                    f"{SUPERSET_URL}/api/v1/dataset/",
                    headers=headers,
                    timeout=10,
                    json={"database": db_id, "schema": schema_name, "table_name": table_name},
                )
                if r.status_code in (200, 201):
                    log.info("Dataset '%s.%s' enregistré.", schema_name, table_name)
                elif "already exists" not in r.text:
                    log.warning("Erreur dataset '%s.%s': %s", schema_name, table_name, r.text)

    except Exception as e:
        log.warning("Registration Superset ignorée (%s)", e)


def wait_for_superset(max_wait: int = 120) -> None:
    deadline = time.time() + max_wait
    log.info("Attente de Superset (%s)…", SUPERSET_URL)
    while time.time() < deadline:
        try:
            if requests.get(f"{SUPERSET_URL}/health", timeout=5).status_code == 200:
                log.info("Superset prêt.")
                return
        except Exception:
            pass
        time.sleep(5)
    log.warning("Superset non prêt après %ds, on continue.", max_wait)


def ensure_database_ready() -> None:
    admin_dsn = PG_DSN.replace("/robotics", "/superset")
    pg = psycopg2.connect(admin_dsn)
    pg.autocommit = True
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'robotics'")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE robotics OWNER superset")
                log.info("Base 'robotics' créée.")
    finally:
        pg.close()

    pg = pg_connect()
    try:
        with pg:
            with pg.cursor() as cur:
                create_schema(cur)
        log.info("Schéma PostgreSQL prêt.")
    finally:
        pg.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()

    log.info("Démarrage — workers: %d, batch: %d, profondeur: %d",
             SFTP_WORKERS, PG_BATCH_SIZE, SFTP_MAX_DEPTH)

    ensure_database_ready()
    wait_for_superset(max_wait=120)
    register_superset()

    if args.watch:
        log.info("Mode watch — intervalle %ds", args.interval)
        while True:
            try:
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
