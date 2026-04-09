#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sftp_sync.py  —  sync incrémental haute performance
====================================================

Première exécution : lit tous les metadata.json (peut prendre du temps).
Exécutions suivantes : lit UNIQUEMENT les fichiers nouveaux ou modifiés
  (comparaison mtime SFTP vs cache PostgreSQL → en général quelques secondes).

Phases :
  1. BFS parallèle sur toutes les layers → collecte (path, mtime)
  2. Filtre contre hdd_scan_cache → garde seulement les nouveautés
  3. Lecture parallèle des seuls fichiers filtrés
  4. Déduplication + batch INSERT PostgreSQL
  5. Mise à jour du cache

Variables d'env :
  SFTP_WORKERS    workers SFTP   défaut: 16
  PG_BATCH_SIZE   lignes/INSERT  défaut: 500
  SFTP_MAX_DEPTH  profondeur max défaut: 8
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
        sftp.get_channel().in_window_size    = 2 * 1024 * 1024
        sftp.get_channel().in_max_packet_size = 32768
        _tlocal.ssh  = ssh
        _tlocal.sftp = sftp
    return _tlocal.sftp


# ── Phase 1 : BFS parallèle avec collecte mtime ───────────────────────────────

def _scan_dir(path: str, depth: int) -> tuple[list[tuple[str, int]], list[tuple[str, float]]]:
    """
    Retourne :
      - sous-répertoires à explorer : [(path, depth), ...]
      - metadata.json trouvés       : [(path, mtime), ...]
    """
    if depth > SFTP_MAX_DEPTH:
        return [], []
    try:
        entries = _get_sftp().listdir_attr(path)
    except Exception as e:
        log.warning("listdir %s : %s", path, e)
        return [], []

    subdirs: list[tuple[str, int]]    = []
    metas:   list[tuple[str, float]]  = []

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
    """BFS parallèle → {layer: [(path, mtime), ...]}"""
    layer_results: dict[str, list[tuple[str, float]]] = {l: [] for l, _ in LAYERS}
    futures: dict = {}
    dirs_scanned = 0

    with ThreadPoolExecutor(max_workers=SFTP_WORKERS, thread_name_prefix="sftp-scan") as ex:
        for layer, base in LAYERS:
            f = ex.submit(_scan_dir, base, 0)
            futures[f] = (layer, base)
        log.info("Scan BFS démarré — %d layers, %d workers", len(LAYERS), SFTP_WORKERS)

        while futures:
            done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
            new_futures: dict = {}
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
    """
    Compare les mtime scannés avec le cache hdd_scan_cache.
    Retourne uniquement les fichiers nouveaux ou dont le mtime a changé.
    """
    # Tous les chemins à vérifier
    all_paths = [
        path
        for paths in layer_results.values()
        for path, _ in paths
    ]
    if not all_paths:
        return {l: [] for l in layer_results}

    pg = pg_connect()
    try:
        with pg.cursor() as cur:
            # Récupère le cache en une seule requête
            cur.execute(
                "SELECT path, mtime FROM hdd_scan_cache WHERE path = ANY(%s)",
                (all_paths,)
            )
            cached = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        pg.close()

    filtered: dict[str, list[tuple[str, float]]] = {}
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
    """Met à jour hdd_scan_cache avec les fichiers traités avec succès."""
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

def _read_meta(layer: str, path: str, mtime: float) -> tuple[str, str, float, dict] | None:
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


def read_metadata_parallel(
    filtered: dict[str, list[tuple[str, float]]]
) -> list[tuple[str, str, float, dict]]:
    all_items = [
        (layer, path, mtime)
        for layer, items in filtered.items()
        for path, mtime in items
    ]
    total = len(all_items)
    if total == 0:
        return []

    results = []
    t0 = time.perf_counter()
    log.info("Lecture de %d fichiers avec %d workers…", total, SFTP_WORKERS)

    with ThreadPoolExecutor(max_workers=SFTP_WORKERS, thread_name_prefix="sftp-read") as ex:
        futures = {
            ex.submit(_read_meta, layer, path, mtime): i
            for i, (layer, path, mtime) in enumerate(all_items)
        }
        for i, f in enumerate(as_completed(futures), start=1):
            result = f.result()
            if result is not None:
                results.append(result)
            if i % 500 == 0 or i == total:
                elapsed = time.perf_counter() - t0
                rate = i / elapsed if elapsed > 0 else 0
                eta = (total - i) / rate if rate > 0 else 0
                log.info("  Lecture %d/%d (%.0f%%) — %.0f f/s — ETA %.0fs",
                         i, total, 100 * i / total, rate, eta)

    log.info("Lecture terminée en %.1fs — %d/%d lus", time.perf_counter() - t0, len(results), total)
    return results


# ── PostgreSQL ────────────────────────────────────────────────────────────────

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
    # Cache incrémental : évite de relire les fichiers déjà traités
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hdd_scan_cache (
            path      TEXT PRIMARY KEY,
            mtime     DOUBLE PRECISION NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_layer      ON hdd_sessions(layer)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_session_id ON hdd_sessions(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_start_time ON hdd_sessions(start_time)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_sessions_meta_gin   ON hdd_sessions USING GIN (metadata_json)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_cameras_layer_sess  ON hdd_cameras(layer, session_id)",
        "CREATE INDEX IF NOT EXISTS idx_hdd_trackers_layer_sess ON hdd_trackers(layer, session_id)",
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


# ── INSERT batch avec déduplication ──────────────────────────────────────────

def flush_sessions(cur, rows: list[dict]) -> None:
    if not rows:
        return
    # Déduplique sur (layer, session_id) — garde la dernière occurrence
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
    # Déduplique sur (layer, session_id, camera_key)
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
    # Déduplique sur (layer, session_id, tracker_key)
    deduped = {(r[0], r[1], r[2]): r for r in rows}
    execute_values(cur, """
        INSERT INTO hdd_trackers (layer, session_id, tracker_key, serial, model)
        VALUES %s
        ON CONFLICT (layer, session_id, tracker_key) DO UPDATE SET
            serial    = EXCLUDED.serial,
            model     = EXCLUDED.model,
            synced_at = NOW()
    """, list(deduped.values()), page_size=PG_BATCH_SIZE)


# ── Sync principal ────────────────────────────────────────────────────────────

def sync_once() -> int:
    t0 = time.perf_counter()

    # Phase 1 : scan BFS (rapide — pas de lecture fichier)
    layer_results = find_all_metadata_parallel()
    total_found = sum(len(v) for v in layer_results.values())
    if total_found == 0:
        log.info("Aucun metadata.json trouvé.")
        return 0

    # Phase 2 : filtre incrémental (ignore les déjà traités)
    filtered = filter_new_or_changed(layer_results)
    total_new = sum(len(v) for v in filtered.values())
    if total_new == 0:
        log.info("Aucun fichier nouveau ou modifié — sync ignoré.")
        return 0

    # Phase 3 : lecture parallèle des seuls fichiers nouveaux
    parsed = read_metadata_parallel(filtered)
    if not parsed:
        log.info("Aucun fichier lu avec succès.")
        return 0

    # Phase 4 : assemblage + déduplication
    session_rows: list[dict]  = []
    camera_rows:  list[tuple] = []
    tracker_rows: list[tuple] = []

    for layer, path, _mtime, meta in parsed:
        fallback = posixpath.basename(posixpath.dirname(path))
        row = parse_metadata(meta, layer, path, fallback)
        session_id = row["session_id"]
        session_rows.append(row)
        camera_rows.extend([
            (layer, session_id, str(k), c.get("name"), c.get("position"), c.get("serial"))
            for k, c in (meta.get("cameras") or {}).items()
        ])
        tracker_rows.extend([
            (layer, session_id, str(k), t.get("serial"), t.get("model"))
            for k, t in (meta.get("trackers") or {}).items()
        ])

    # Phase 5 : batch INSERT PostgreSQL
    t_pg = time.perf_counter()
    pg  = pg_connect()
    cur = pg.cursor()
    try:
        for i in range(0, len(session_rows), PG_BATCH_SIZE):
            flush_sessions(cur, session_rows[i:i + PG_BATCH_SIZE])
        pg.commit()

        for i in range(0, len(camera_rows), PG_BATCH_SIZE):
            flush_cameras(cur, camera_rows[i:i + PG_BATCH_SIZE])
        pg.commit()

        for i in range(0, len(tracker_rows), PG_BATCH_SIZE):
            flush_trackers(cur, tracker_rows[i:i + PG_BATCH_SIZE])
        pg.commit()

        cur.close()
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()

    log.info("INSERT en %.1fs — %d sessions, %d caméras, %d trackers",
             time.perf_counter() - t_pg,
             len(session_rows), len(camera_rows), len(tracker_rows))

    # Phase 6 : mise à jour cache (seulement les fichiers insérés avec succès)
    processed = [(path, mtime) for _, path, mtime, _ in parsed]
    update_scan_cache(processed)

    log.info("Sync complet en %.1fs total", time.perf_counter() - t0)
    return len(session_rows)


# ── Superset ──────────────────────────────────────────────────────────────────

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
                "database_name": SUPERSET_DB_NAME, "sqlalchemy_uri": SUPERSET_DB_URI,
                "expose_in_sqllab": True, "allow_run_async": True, "allow_dml": False,
            })
            db_id = r.json().get("id") if r.status_code in (200, 201) else None
            if not db_id:
                log.warning("Erreur enregistrement DB Superset : %s", r.text)
                return
            log.info("Base '%s' enregistrée (id=%s)", SUPERSET_DB_NAME, db_id)
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
                elif "already exists" not in r.text:
                    log.warning("Erreur dataset '%s': %s", table_name, r.text)

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
    # Crée la base robotics si absente
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

    # Crée les tables/index
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
    parser.add_argument("--watch",    action="store_true")
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
            except Exception as e:
                log.error("Erreur sync : %s", e)
            time.sleep(args.interval)
    else:
        sync_once()


if __name__ == "__main__":
    main()
