import json
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError
from flask import Blueprint, jsonify

metadata_bp = Blueprint("metadata", __name__)

_WEBHDFS_BASE = "http://namenode:9870/webhdfs/v1"
_RAW_BASE = "/sessions"


def _webhdfs_url(path: str, op: str, **params) -> str:
    clean = path if path.startswith("/") else f"/{path}"
    query = {"op": op}
    query.update(params)
    return f"{_WEBHDFS_BASE}{clean}?{urlencode(query)}"


def _webhdfs_list(path: str, timeout: int = 20) -> list[dict]:
    url = _webhdfs_url(path, "LISTSTATUS")
    with urlopen(url, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload.get("FileStatuses", {}).get("FileStatus", [])


def _webhdfs_read(path: str, timeout: int = 20) -> str:
    url = _webhdfs_url(path, "OPEN")
    with urlopen(url, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def _normalize_session_dir(session_id: str) -> str:
    return session_id if session_id.startswith("session_") else f"session_{session_id}"


def _read_session_metadata(session_id: str, timeout: int = 15) -> dict:
    session_dir = _normalize_session_dir(session_id)
    metadata_path = f"{_RAW_BASE}/{session_dir}/metadata.json"

    raw = _webhdfs_read(metadata_path, timeout=timeout)
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("metadata.json n'est pas un objet JSON")

    return {
        "session_dir": session_dir,
        "metadata": parsed,
        "metadata_error": None,
    }


@metadata_bp.route("/api/metadata/sessions", methods=["GET"])
def list_sessions_metadata():
    try:
        status_rows = _webhdfs_list(_RAW_BASE, timeout=30)
        rows = []
        dynamic_keys = set()
        for entry in status_rows:
            if entry.get("type") != "DIRECTORY":
                continue
            session_dir = str(entry.get("pathSuffix", "")).strip()
            if not session_dir.startswith("session_"):
                continue

            metadata_path = f"{_RAW_BASE}/{session_dir}/metadata.json"
            metadata = None
            metadata_error = None
            try:
                raw = _webhdfs_read(metadata_path, timeout=15)
                metadata = json.loads(raw)
            except Exception as e:
                metadata_error = str(e)

            if isinstance(metadata, dict):
                dynamic_keys.update(metadata.keys())
                rows.append({"session_dir": session_dir, "metadata": metadata, "metadata_error": None})
            else:
                rows.append({"session_dir": session_dir, "metadata": None, "metadata_error": metadata_error})

        def _sort_key(row: dict):
            metadata = row.get("metadata") or {}
            session_id = str(metadata.get("session_id") or "")
            return session_id or row.get("session_dir", "")

        rows.sort(key=_sort_key, reverse=True)
        keys = sorted(dynamic_keys)
        return jsonify({"sessions": rows, "count": len(rows), "keys": keys})
    except HTTPError as e:
        if e.code == 404:
            return jsonify({"sessions": [], "count": 0, "keys": []})
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@metadata_bp.route("/api/metadata/sessions/<session_id>", methods=["GET"])
def get_session_metadata(session_id: str):
    try:
        row = _read_session_metadata(session_id)
        return jsonify(row)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
