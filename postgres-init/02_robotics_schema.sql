-- ── Base robotics ──────────────────────────────────────────────────────────────
SELECT 'CREATE DATABASE robotics OWNER superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'robotics')\gexec

\connect robotics

-- ── Sessions HDD ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hdd_sessions (
    session_id       TEXT PRIMARY KEY,
    scenario         TEXT,
    start_time       TIMESTAMPTZ,
    end_time         TIMESTAMPTZ,
    trigger_time     TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    failed           BOOLEAN,
    video_width      INTEGER,
    video_height     INTEGER,
    video_fps        INTEGER,
    layer            TEXT,          -- bronze | silver | gold | inbox
    synced_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ── Cameras par session ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hdd_cameras (
    session_id  TEXT,
    camera_key  TEXT,               -- "0", "1", "2"
    name        TEXT,
    position    TEXT,               -- left | right | head
    serial      TEXT,
    PRIMARY KEY (session_id, camera_key)
);

-- ── Trackers VIVE par session ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hdd_trackers (
    session_id  TEXT,
    tracker_key TEXT,               -- "1", "2", "3"
    serial      TEXT,
    model       TEXT,
    PRIMARY KEY (session_id, tracker_key)
);

-- ── Vue dénormalisée pratique pour Superset ────────────────────────────────────
CREATE OR REPLACE VIEW v_sessions_full AS
SELECT
    s.session_id,
    s.scenario,
    s.start_time,
    s.end_time,
    s.trigger_time,
    s.duration_seconds,
    s.failed,
    s.video_width,
    s.video_height,
    s.video_fps,
    s.layer,
    s.synced_at,
    COUNT(DISTINCT c.camera_key) AS camera_count,
    COUNT(DISTINCT t.tracker_key) AS tracker_count
FROM hdd_sessions s
LEFT JOIN hdd_cameras  c USING (session_id)
LEFT JOIN hdd_trackers t USING (session_id)
GROUP BY s.session_id;
