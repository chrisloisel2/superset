"""
Superset configuration — Hadoop Red Stack
Toute la config passe par ce seul fichier.
Adapté à l'architecture : HiveServer2 (NOSASL), PostgreSQL metastore,
réseau Docker hadoop-red-version-default.
"""

import os

# ─── Clé secrète ────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "hadoop_red_superset_secret_2024!")

# ─── Base de données Superset (PostgreSQL partagé) ──────────────────────────
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://hive:hive123@postgres:5432/superset"
)

# ─── Cache ───────────────────────────────────────────────────────────────────
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
DATA_CACHE_CONFIG = CACHE_CONFIG

# ─── Serveur ─────────────────────────────────────────────────────────────────
ROW_LIMIT = 50000
SUPERSET_WEBSERVER_PORT = 8088
SUPERSET_WEBSERVER_TIMEOUT = 300

# ─── Sécurité ────────────────────────────────────────────────────────────────
WTF_CSRF_ENABLED = True
# L'endpoint guest-token (embed) doit être exempt de CSRF
WTF_CSRF_EXEMPT_LIST = ["superset.views.core.log", "superset.charts.api.data"]
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_SAMESITE = "None"   # requis pour l'embed cross-origin
TALISMAN_ENABLED = False

# ─── CORS — autoriser le site web à interroger l'API Superset ────────────────
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

# ─── Content Security Policy — autoriser l'iframe depuis le site web ─────────
TALISMAN_CONFIG = {
    "force_https": False,
    "content_security_policy": {
        "frame-ancestors": ["'self'", "http://192.168.88.27:23000", "http://localhost:23000"],
    },
}

# ─── Feature flags ───────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ALERT_REPORTS": False,
    "EMBEDDED_SUPERSET": True,        # Active l'embed des dashboards
    "EMBEDDABLE_CHARTS": True,
}

# ─── Connexion Hive (HiveServer2 NOSASL) ─────────────────────────────────────
HIVE_CONNECTION = {
    "sqlalchemy_uri": "hive://hiveserver2:10000/robotics?auth=NONE",
    "extra": '{"connect_args": {"auth": "NOSASL"}}',
}

# ─── Connexion MongoDB ────────────────────────────────────────────────────────
# URI externe : l'instance MongoDB tourne sur 192.168.88.17:27017
MONGODB_CONNECTION = {
    "sqlalchemy_uri": "mongodb://admin:admin123@192.168.88.17:27017/",
}

# ─── Spool / Proxy (192.168.88.5) ────────────────────────────────────────────
SPOOL_HOST = os.environ.get("SPOOL_HOST", "192.168.88.5")
SPOOL_USER = os.environ.get("SPOOL_USER", "Admin123456")
HTTP_PROXY  = os.environ.get("HTTP_PROXY",  f"http://{SPOOL_HOST}:3128")
HTTPS_PROXY = os.environ.get("HTTPS_PROXY", f"http://{SPOOL_HOST}:3128")

# ─── Upload de fichiers CSV / Excel ──────────────────────────────────────────
UPLOAD_FOLDER = "/app/superset_home/uploads/"
ALLOWED_EXTENSIONS = {"csv", "tsv", "txt", "xlsx", "xls"}
UPLOAD_ENABLED = True
CSV_EXPORT = True

# ─── Localisation ────────────────────────────────────────────────────────────
BABEL_DEFAULT_LOCALE = "fr"
BABEL_DEFAULT_TIMEZONE = "Europe/Paris"

# ─── Async queries ────────────────────────────────────────────────────────────
RESULTS_BACKEND = None

# ─── Logging ─────────────────────────────────────────────────────────────────
ENABLE_TIME_ROTATE = False
