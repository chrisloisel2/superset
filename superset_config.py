"""
SuperHive — configuration unique Superset
Hive · MongoDB · AWS S3 · Spool 192.168.88.5 · Embed 192.168.88.27:23000
"""
import os

# ── Sécurité ──────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "hadoop_red_superset_secret_2024!")

# ── Meta-DB Superset (PostgreSQL partagé avec Hive metastore) ─────────────────
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://hive:hive123@postgres:5432/superset",
)

# ── Cache simple (pas de Redis nécessaire) ────────────────────────────────────
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG

# ── Serveur ───────────────────────────────────────────────────────────────────
ROW_LIMIT = 50_000
SUPERSET_WEBSERVER_PORT = 8088
SUPERSET_WEBSERVER_TIMEOUT = 300

# ── Sécurité web ──────────────────────────────────────────────────────────────
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = ["superset.views.core.log", "superset.charts.api.data"]
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_SAMESITE = "None"   # cross-origin iframe
TALISMAN_ENABLED = False

# ── CORS : autoriser le site web ──────────────────────────────────────────────
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": [
        "http://192.168.88.27:23000",
        "http://localhost:23000",
    ],
}

# ── CSP : autoriser l'iframe depuis le site web ───────────────────────────────
TALISMAN_CONFIG = {
    "force_https": False,
    "content_security_policy": {
        "frame-ancestors": ["'self'", "http://192.168.88.27:23000", "http://localhost:23000"],
    },
}

# ── Feature flags ─────────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "EMBEDDED_SUPERSET": True,
    "EMBEDDABLE_CHARTS": True,
    "ALERT_REPORTS": False,
}

# ── Proxy sortant via spool (192.168.88.5) ────────────────────────────────────
_spool = os.environ.get("SPOOL_HOST", "192.168.88.5")
HTTP_PROXY  = os.environ.get("HTTP_PROXY",  f"http://{_spool}:3128")
HTTPS_PROXY = os.environ.get("HTTPS_PROXY", f"http://{_spool}:3128")

# ── AWS S3 (boto3 / s3fs utilisés par Superset pour les uploads vers S3) ──────
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION    = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")
AWS_S3_BUCKET         = os.environ.get("AWS_S3_BUCKET", "")

# ── Upload fichiers ───────────────────────────────────────────────────────────
UPLOAD_FOLDER = "/app/superset_home/uploads/"
ALLOWED_EXTENSIONS = {"csv", "tsv", "txt", "xlsx", "xls"}
UPLOAD_ENABLED = True
CSV_EXPORT = True

# ── Localisation ─────────────────────────────────────────────────────────────
BABEL_DEFAULT_LOCALE = "fr"
BABEL_DEFAULT_TIMEZONE = "Europe/Paris"

# ── Async ─────────────────────────────────────────────────────────────────────
RESULTS_BACKEND = None
ENABLE_TIME_ROTATE = False
