#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sftp_sync.py  —  version haute performance
==========================================
Phase 1 : BFS parallèle sur toutes les layers simultanément
          Chaque worker SFTP possède sa propre connexion SSH/SFTP (threading.local).
          Les sous-répertoires sont distribués dynamiquement entre les workers
          au fur et à mesure de la découverte → latence réseau masquée.

Phase 2 : Lecture parallèle de tous les metadata.json collectés.
          Même pool de connexions thread-local, zéro overhead de reconnexion.

Phase 3 : Batch INSERT unique dans PostgreSQL.
          Une seule transaction par table (sessions / cameras / trackers).

Variables d'env de performance :
  SFTP_WORKERS    workers SFTP scan+lecture    défaut: 8
  PG_BATCH_SIZE   lignes par INSERT batch      défaut: 500
  SFTP_MAX_DEPTH  profondeur max de recherche  défaut: 8
"""

import argparse
import json
import logging
import os
import posixpath
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
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

# ── Configuration performance ─────────────────────────────────────────────────

SFTP_WORKERS   = int(os.environ.get("SFTP_WORKERS",   "8"))
PG_BATCH_SIZE  = int(os.environ.get("PG_BATCH_SIZE",  "500"))
SFTP_MAX_DEPTH = int(os.environ.get("SFTP_MAX_DEPTH", "8"))

# ── Configuration PostgreSQL ──────────────────────────────────────────────────

PG_DSN = os.environ.get(
    "ROBOTICS_DB_URI",
    "postgresql://superset:superset123@postgres:5432/robotics",
)

# ── Configuration Superset ────────────────────────────────────────────────────

SUPERSET_URL     = os.environ.get("SUPERSET_URL",      "http://superset:8088")
SUPERSET_USER    = os.environ.get("SUPERSET_USER",     "admin")
SUPERSET_PASS    = os.environ.get("SUPERSET_PASSWORD", "Admin123456")
SUPERSET_DB_NAME = os.environ.get("SUPERSET_DB_NAME",  "HDD Robotics")
SUPERSET_DB_URI  = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://superset:superset123@postgres:5432/robotics",
)

# ── Connexions SFTP thread-local ──────────────────────────────────────────────
# Chaque worker du ThreadPoolExecutor crée sa propre connexion SSH/SFTP
# à la première utilisation et la réutilise pour toutes les requêtes suivantes.
# Paramiko n'est pas thread-safe : une connexion par thread est obligatoire.

_tlocal = threading.local()


def _get_sftp() -> paramiko.SFTPClient:
    """Retourne la connexion SFTP du thread courant (crée si absente)."""
    if not getattr(_tlocal, "sftp", None):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            HDD_HOST,
            port=HDD_PORT,
            username=HDD_USER,
            password=HDD_PASSWORD,
            compress=False,          # LAN → compression inutile, ajoute CPU
            timeout=15,
            banner_timeout=15,
            auth_timeout=15,
        )
        sftp = ssh.open_sftp()
        sftp.get_channel().in_window_size = 2 * 1024 * 1024   # 2 MiB window
        sftp.get_channel().in_max_packet_size = 32768
        _tlocal.ssh  = ssh
        _tlocal.sftp = sftp
        log.debug("Nouvelle connexion SFTP dans thread %s", threading.current_thread().name)
    return _tlocal.sftp


# ── Phase 1 : BFS parallèle ───────────────────────────────────────────────────

def _scan_dir(path: str, depth: int) -> tuple[list[tuple[str, int]], list[str]]:
    """
    Liste un répertoire via la connexion SFTP thread-local.
    Retourne (sous-répertoires, chemins_metadata_json).
    """
    if depth > SFTP_MAX_DEPTH:
        return [], []

    try:
        entries = _get_sftp().listdir_attr(path)
    except Exception as e:
        log.warning("listdir %s : %s", path, e)
        return [], []

    subdirs:    list[tuple[str, int]] = []
    meta_paths: list[str]             = []

    for entry in entries:
        full = posixpath.join(path, entry.filename)
        try:
            if S_ISDIR(entry.st_mode):
                subdirs.append((full, depth + 1))
            elif entry.filename == "metadata.json":
                meta_paths.append(full)
        except Exception:
            pass

    return subdirs, meta_paths


def find_all_metadata_parallel() -> dict[str, list[str]]:
    """
    BFS parallèle sur toutes les layers simultanément.
    Retourne {layer: [path_metadata_json, ...]}.

    Algorithme :
      - Soumet les racines de toutes les layers comme premières tâches.
      - Quand un worker termine, les sous-répertoires découverts sont
        immédiatement soumis comme nouvelles tâches (dynamique).
      - wait(FIRST_COMPLETED) évite de bloquer si des workers sont libres.
    """
    layer_paths: dict[str, list[str]] = {layer: [] for layer, _ in LAYERS}
    # future → (layer, path) pour retrouver le contexte au retour
    futures: dict = {}

    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=SFTP_WORKERS, thread_name_prefix="sftp-scan") as ex:

        # Amorce : soumet les racines de toutes les layers
        for layer, base in LAYERS:
            f = ex.submit(_scan_dir, base, 0)
            futures[f] = (layer, base)

        while futures:
            done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
            new_futures: dict = {}
            for f in done:
                layer, path = futures.pop(f)
                try:
                    subdirs, meta_paths = f.result()
                    layer_paths[layer].extend(meta_paths)
                    for subdir, depth in subdirs:
                        nf = ex.submit(_scan_dir, subdir, depth)
                        new_futures[nf] = (layer, subdir)
                except Exception as e:
                    log.warning("Erreur scan %s : %s", path, e)
            futures.update(new_futures)

    elapsed = time.perf_counter() - t0
    total = sum(len(v) for v in layer_paths.values())
    log.info("Scan terminé en %.2fs — %d metadata.json trouvés", elapsed, total)
    for layer, paths in layer_paths.items():
        log.info("  [%s] %d sessions", layer, len(paths))

    return layer_paths


# ── Phase 2 : lecture parallèle des metadata.json ────────────────────────────

def _read_meta(layer: str, path: str) -> tuple[str, str, dict] | None:
    """Lit et parse un metadata.json. Retourne (layer, path, dict) ou None."""
    try:
        buf = BytesIO()
        _get_sftp().getfo(path, buf)
        return layer, path, json.loads(buf.getvalue().decode("utf-8"))
    except FileNotFoundError:
        log.debug("Disparu pendant la lecture : %s", path)
        return None
    except Exception as e:
        log.warning("Lecture %s : %s", path, e)
        return None


def read_all_metadata_parallel(layer_paths: dict[str, list[str]]) -> list[tuple[str, str, dict]]:
    """
    Lit tous les metadata.json en parallèle via le même pool de connexions.
    Retourne [(layer, path, meta_dict), ...].
    """
    all_paths = [
        (layer, path)
        for layer, paths in layer_paths.items()
        for path in paths
    ]
    total = len(all_paths)
    if total == 0:
        return []

    results: list[tuple[str, str, dict]] = []
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=SFTP_WORKERS, thread_name_prefix="sftp-read") as ex:
        futures = {ex.submit(_read_meta, layer, path): (layer, path) for layer, path in all_paths}
        for i, f in enumerate(as_completed(futures), start=1):
            result = f.result()
            if result is not None:
                results.append(result)
            if i % 50 == 0 or i == total:
                log.info("  Lecture %d/%d (%.0f%%)", i, total, 100 * i / total)

    elapsed = time.perf_counter() - t0
    log.info("Lecture terminée en %.2fs — %d/%d lus", elapsed, len(results), total)
    return results


# ── Helpers PostgreSQL ────────────────────────────────────────────────────────

def pg_connect():
    return psycopg2.connect(PG_DSN)


def create_schema(cur) -> None:
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
            CONSTRAINT uq_hdd_sessions_layer_session UNIQUE (layer, session_id)
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
    # Index pour les requêtes Superset courantes
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_layer       ON hdd_sessions(layer)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_session_id  ON hdd_sessions(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_start_time  ON hdd_sessions(start_time)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_meta_gin    ON hdd_sessions USING GIN (metadata_json)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_cameras_layer_sess   ON hdd_cameras(layer, session_id)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_trackers_layer_sess  ON hdd_trackers(layer, session_id)",
    ]:
        cur.execute(ddl)

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


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_metadata(meta: dict, layer: str, source_path: str, fallback_session_id: str) -> dict:
    vc = meta.get("video_config") or {}
    return {
        "layer":            layer,
        "session_id":       meta.get("session_id") or fallback_session_id,
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


def extract_camera_rows(layer: str, session_id: str, cameras: dict) -> list[tuple]:
    return [
        (layer, session_id, str(key), cam.get("name"), cam.get("position"), cam.get("serial"))
        for key, cam in cameras.items()
    ]


def extract_tracker_rows(layer: str, session_id: str, trackers: dict) -> list[tuple]:
    return [
        (layer, session_id, str(key), t.get("serial"), t.get("model"))
        for key, t in trackers.items()
    ]


# ── Phase 3 : batch INSERT PostgreSQL ────────────────────────────────────────

def flush_sessions(cur, rows: list[dict]) -> None:
    if not rows:
        return
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
    ) for r in rows], page_size=PG_BATCH_SIZE)


def flush_cameras(cur, rows: list[tuple]) -> None:
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO hdd_cameras (layer, session_id, camera_key, name, position, serial)
        VALUES %s
        ON CONFLICT (layer, session_id, camera_key) DO UPDATE SET
            name      = EXCLUDED.name,
            position  = EXCLUDED.position,
            serial    = EXCLUDED.serial,
            synced_at = NOW()
    """, rows, page_size=PG_BATCH_SIZE)


def flush_trackers(cur, rows: list[tuple]) -> None:
    if not rows:
        return
    execute_values(cur, """
        INSERT INTO hdd_trackers (layer, session_id, tracker_key, serial, model)
        VALUES %s
        ON CONFLICT (layer, session_id, tracker_key) DO UPDATE SET
            serial    = EXCLUDED.serial,
            model     = EXCLUDED.model,
            synced_at = NOW()
    """, rows, page_size=PG_BATCH_SIZE)


# ── Sync principal ────────────────────────────────────────────────────────────

def sync_once() -> int:
    t_total = time.perf_counter()

    # ── Phase 1 : découverte parallèle ────────────────────────────────────────
    layer_paths = find_all_metadata_parallel()
    total_found = sum(len(v) for v in layer_paths.values())
    if total_found == 0:
        log.info("Aucune session trouvée.")
        return 0

    # ── Phase 2 : lecture parallèle ───────────────────────────────────────────
    parsed = read_all_metadata_parallel(layer_paths)

    # ── Assemblage des lignes ─────────────────────────────────────────────────
    session_rows: list[dict]   = []
    camera_rows:  list[tuple]  = []
    tracker_rows: list[tuple]  = []

    for layer, path, meta in parsed:
        fallback_id = posixpath.basename(posixpath.dirname(path))
        row = parse_metadata(meta, layer, path, fallback_id)
        session_id = row["session_id"]
        session_rows.append(row)
        camera_rows.extend(extract_camera_rows(layer, session_id, meta.get("cameras")  or {}))
        tracker_rows.extend(extract_tracker_rows(layer, session_id, meta.get("trackers") or {}))

    # ── Phase 3 : INSERT PostgreSQL ───────────────────────────────────────────
    t_pg = time.perf_counter()
    pg  = pg_connect()
    cur = pg.cursor()
    try:
        for i in range(0, len(session_rows), PG_BATCH_SIZE):
            flush_sessions(cur, session_rows[i : i + PG_BATCH_SIZE])
        pg.commit()

        for i in range(0, len(camera_rows), PG_BATCH_SIZE):
            flush_cameras(cur, camera_rows[i : i + PG_BATCH_SIZE])
        pg.commit()

        for i in range(0, len(tracker_rows), PG_BATCH_SIZE):
            flush_trackers(cur, tracker_rows[i : i + PG_BATCH_SIZE])
        pg.commit()

        cur.close()
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()

    log.info(
        "INSERT terminé en %.2fs — %d sessions, %d caméras, %d trackers",
        time.perf_counter() - t_pg,
        len(session_rows), len(camera_rows), len(tracker_rows),
    )
    log.info("Sync complet en %.2fs", time.perf_counter() - t_total)
    return len(session_rows)


# ── Enregistrement Superset ───────────────────────────────────────────────────

def register_superset() -> None:
    try:
        sess = requests.Session()
        resp = sess.post(f"{SUPERSET_URL}/api/v1/security/login", json={
            "username": SUPERSET_USER, "password": SUPERSET_PASS,
            "provider": "db", "refresh": True,
        }, timeout=10)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            log.warning("Superset login échoué, registration ignorée.")
            return

        headers = {"Authorization": f"Bearer {token}"}
        csrf = sess.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers, timeout=10)
        csrf.raise_for_status()
        headers["X-CSRFToken"] = csrf.json().get("result", "")
        headers["Referer"] = SUPERSET_URL

        dbs = sess.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers, timeout=10).json()
        existing = [d for d in dbs.get("result", []) if d.get("database_name") == SUPERSET_DB_NAME]

        if not existing:
            r = sess.post(f"{SUPERSET_URL}/api/v1/database/", headers=headers, timeout=10, json={
                "database_name":    SUPERSET_DB_NAME,
                "sqlalchemy_uri":   SUPERSET_DB_URI,
                "expose_in_sqllab": True,
                "allow_run_async":  True,
                "allow_dml":        False,
            })
            db_id = r.json()["id"] if r.status_code in (200, 201) else None
            if db_id:
                log.info("Base '%s' enregistrée (id=%s)", SUPERSET_DB_NAME, db_id)
            else:
                log.warning("Erreur enregistrement DB : %s", r.text)
                return
        else:
            db_id = existing[0]["id"]
            log.info("Base '%s' déjà présente (id=%s)", SUPERSET_DB_NAME, db_id)

        for table_name in ("hdd_sessions", "hdd_cameras", "hdd_trackers", "v_sessions_full"):
            ds = sess.get(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers, timeout=10,
                params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "val": table_name}]})},
            ).json()
            if ds.get("count", 0) == 0:
                r = sess.post(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers, timeout=10,
                    json={"database": db_id, "schema": "public", "table_name": table_name})
                if r.status_code in (200, 201):
                    log.info("Dataset '%s' enregistré.", table_name)
                else:
                    log.warning("Erreur dataset '%s': %s", table_name, r.text)
            else:
                log.info("Dataset '%s' déjà présent.", table_name)

    except Exception as e:
        log.warning("Registration Superset ignorée (%s)", e)


def wait_for_superset(max_wait: int = 120) -> None:
    log.info("Attente de Superset (%s)…", SUPERSET_URL)
    deadline = time.time() + max_wait
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
    pg = pg_connect()
    try:
        with pg:
            with pg.cursor() as cur:
                create_schema(cur)
        log.info("Schéma PostgreSQL prêt.")
    finally:
        pg.close()


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Sync SFTP HDD → PostgreSQL (parallèle)")
    parser.add_argument("--watch",    action="store_true", help="Boucle continue")
    parser.add_argument("--interval", type=int, default=60, help="Intervalle en secondes")
    args = parser.parse_args()

    log.info(
        "Démarrage — workers SFTP: %d, batch PG: %d, profondeur max: %d",
        SFTP_WORKERS, PG_BATCH_SIZE, SFTP_MAX_DEPTH,
    )

    ensure_database_ready()
    wait_for_superset(max_wait=120)
    register_superset()

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
