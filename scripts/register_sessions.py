#!/usr/bin/env python3
"""
register_sessions.py
====================
Scans HDFS /sessions/, copies each CSV to the Hive warehouse layout,
and registers Hive partitions via HiveServer2.

Why copy?
  Each session folder contains 3 different CSV files (tracker_positions,
  pince1_data, pince2_data). If a Hive partition pointed directly at the
  session folder it would try to parse all CSVs as a single table schema,
  producing corrupt results. We copy each file to a table-specific path:

    hdfs:///hive/sessions/tracker_positions/session_id=<id>/tracker_positions.csv
    hdfs:///hive/sessions/pince1_data/session_id=<id>/pince1_data.csv
    hdfs:///hive/sessions/pince2_data/session_id=<id>/pince2_data.csv

Usage (run inside the flask-api or hiveserver2 container):
    python3 /scripts/register_sessions.py

    # Or from the Docker host:
    docker exec hiveserver2-red python3 /scripts/register_sessions.py
"""

import re
import subprocess
import sys
from pyhive import hive

# ── Configuration ─────────────────────────────────────────────────────────────
HIVE_HOST      = "hiveserver2"
HIVE_PORT      = 10000
HIVE_DATABASE  = "robotics"
HIVE_AUTH      = "NOSASL"

# Use absolute HDFS paths to avoid fs defaultFS mismatches across containers.
HDFS_FS        = "hdfs://namenode:9000"
HDFS_SESSIONS  = "/sessions"       # raw session data (on HDFS)
HDFS_HIVE_BASE = "/hive/sessions"  # Hive warehouse layout (on HDFS)

TABLES = {
    "tracker_positions": "tracker_positions.csv",
    "pince1_data":       "pince1_data.csv",
    "pince2_data":       "pince2_data.csv",
}

SESSION_RE = re.compile(r"^session_\d{8}_\d{6}$")

# ── HDFS helpers ──────────────────────────────────────────────────────────────

def hdfs_ls(path: str) -> list[str]:
    result = subprocess.run(
        ["hdfs", "dfs", "-fs", HDFS_FS, "-ls", path],
        capture_output=True, text=True
    )
    entries = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 8:
            entries.append(parts[-1].split("/")[-1])
    return entries


def hdfs_mkdir(path: str):
    subprocess.run(["hdfs", "dfs", "-fs", HDFS_FS, "-mkdir", "-p", path], check=True)


def hdfs_cp(src: str, dst_dir: str):
    subprocess.run(["hdfs", "dfs", "-fs", HDFS_FS, "-cp", "-f", src, dst_dir], check=True)


def hdfs_exists(path: str) -> bool:
    result = subprocess.run(["hdfs", "dfs", "-fs", HDFS_FS, "-test", "-e", path])
    return result.returncode == 0

# ── Hive helpers ──────────────────────────────────────────────────────────────

def get_hive_connection():
    return hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DATABASE,
        auth=HIVE_AUTH,
    )


def add_partition(cursor, table: str, session_id: str, location: str):
    sql = (
        f"ALTER TABLE {HIVE_DATABASE}.{table} "
        f"ADD IF NOT EXISTS PARTITION (session_id='{session_id}') "
        f"LOCATION '{location}'"
    )
    cursor.execute(sql)
    print(f"  [+] Partition enregistrée : {table} / {session_id}")

# ── Session processing ────────────────────────────────────────────────────────

def register_session(cursor, session_name: str):
    session_id = session_name  # e.g. "session_20260222_175519"
    src_base   = f"{HDFS_SESSIONS}/{session_name}"
    print(f"\nTraitement : {session_name}")

    for table, csv_file in TABLES.items():
        src_file = f"{src_base}/{csv_file}"
        dst_dir  = f"{HDFS_HIVE_BASE}/{table}/session_id={session_id}"
        dst_file = f"{dst_dir}/{csv_file}"

        if not hdfs_exists(src_file):
            print(f"  [!] Fichier absent dans HDFS : {src_file}")
            continue

        hdfs_mkdir(dst_dir)

        if not hdfs_exists(dst_file):
            print(f"  Copie {csv_file} -> {dst_dir}")
            hdfs_cp(src_file, dst_dir)
        else:
            print(f"  Déjà présent : {csv_file}")

        location_uri = f"{HDFS_FS}{dst_dir}"
        add_partition(cursor, table, session_id, location_uri)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Register Sessions Script ===")
    print(f"Scan HDFS : {HDFS_SESSIONS}")

    session_dirs = [
        name for name in hdfs_ls(HDFS_SESSIONS)
        if SESSION_RE.match(name)
    ]

    if not session_dirs:
        print("Aucune session trouvée. Vérifiez que les données sont uploadées dans HDFS.")
        sys.exit(1)

    print(f"Sessions trouvées ({len(session_dirs)}) : {session_dirs}")

    conn   = get_hive_connection()
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    cursor.execute(f"USE {HIVE_DATABASE}")

    success = 0
    for session_name in sorted(session_dirs):
        try:
            register_session(cursor, session_name)
            success += 1
        except Exception as e:
            print(f"  [ERREUR] {session_name}: {e}")

    cursor.close()
    conn.close()
    print(f"\n=== Terminé : {success}/{len(session_dirs)} sessions enregistrées ===")


if __name__ == "__main__":
    main()
