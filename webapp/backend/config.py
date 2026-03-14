import os

HIVE_HOST     = os.environ.get("HIVE_HOST", "hiveserver2")
HIVE_PORT     = int(os.environ.get("HIVE_PORT", 10000))
HIVE_DATABASE = os.environ.get("HIVE_DATABASE", "robotics")
HIVE_AUTH     = os.environ.get("HIVE_AUTH", "NONE")
HIVE_SOCKET_TIMEOUT_SECONDS = int(os.environ.get("HIVE_SOCKET_TIMEOUT_SECONDS", 12))


def get_cors_allowed_origins():
    raw = os.environ.get("CORS_ALLOWED_ORIGINS", "*").strip()
    if raw == "*" or raw == "":
        return "*"
    return [origin.strip() for origin in raw.split(",") if origin.strip()]
