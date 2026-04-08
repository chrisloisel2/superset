"""
SuperHive — configuration Superset
Hive externe : 192.168.88.5:20000 (spool)
MongoDB      : 192.168.88.17:27017
AWS S3       : via variables d'env
Embed        : http://192.168.88.27:23000
"""
import os

# ── Sécurité ──────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "hadoop_red_superset_secret_2024!")

# ── Meta-DB Superset (PostgreSQL local) ───────────────────────────────────────
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://superset:superset123@postgres:5432/superset",
)

# ── Cache ─────────────────────────────────────────────────────────────────────
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
SESSION_COOKIE_SAMESITE = "Lax"
TALISMAN_ENABLED = False
RESULTS_BACKEND = None
ENABLE_TIME_ROTATE = False
WTF_CSRF_SSL_STRICT = False
ENABLE_PROXY_FIX = True
SECRET_KEY = "hadoop_red_superset_secret_2024!"

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

# ── CSP : autoriser l'iframe ──────────────────────────────────────────────────
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

# ── Connexion Hive (HiveServer2 via spool 192.168.88.5:20000) ─────────────────
_hive_host = os.environ.get("HIVE_HOST", "192.168.88.5")
_hive_port = os.environ.get("HIVE_PORT", "20000")
HIVE_SQLALCHEMY_URI = f"hive://{_hive_host}:{_hive_port}/robotics?auth=NONE"

# ── Connexion MongoDB (192.168.88.17:27017) ───────────────────────────────────
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://admin:admin123@192.168.88.17:27017/")

# ── AWS S3 ────────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION    = os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")
AWS_S3_BUCKET         = os.environ.get("AWS_S3_BUCKET", "")

# ── Upload fichiers ───────────────────────────────────────────────────────────
UPLOAD_FOLDER = "/app/superset_home/uploads/"
ALLOWED_EXTENSIONS = {"csv", "tsv", "txt", "xlsx", "xls"}
UPLOAD_ENABLED = True
CSV_EXPORT = {"encoding": "utf-8"}

# ── Localisation ─────────────────────────────────────────────────────────────
BABEL_DEFAULT_LOCALE = "fr"
BABEL_DEFAULT_TIMEZONE = "Europe/Paris"
