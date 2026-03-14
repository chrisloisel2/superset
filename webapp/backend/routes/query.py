from flask import Blueprint, jsonify, request
import hive_client

query_bp = Blueprint("query", __name__)

ALLOWED_PREFIXES = ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN")


@query_bp.route("/api/query", methods=["POST"])
def run_query():
    body = request.get_json(force=True)
    sql  = (body.get("sql") or "").strip()

    if not sql:
        return jsonify({"error": "Missing 'sql' field"}), 400

    if not any(sql.upper().lstrip().startswith(p) for p in ALLOWED_PREFIXES):
        return jsonify({"error": "Only read-only queries are permitted (SELECT, SHOW, DESCRIBE, EXPLAIN)"}), 403

    try:
        rows = hive_client.run_query(sql)
        return jsonify({"data": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
