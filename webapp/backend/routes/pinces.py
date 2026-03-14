from flask import Blueprint, jsonify, request
import hive_client

pinces_bp = Blueprint("pinces", __name__)


def _pince_route(session_id: str, table: str):
    limit  = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))
    limit  = max(1, min(limit, 100))
    offset = max(0, offset)
    sql = f"""
        SELECT `timestamp`, time_seconds, t_ms, pince_id, sw, ouverture_mm, angle_deg
        FROM robotics.{table}
        WHERE session_id = '{session_id}'
        LIMIT {limit} OFFSET {offset}
    """
    try:
        rows = hive_client.run_query(sql)
        return jsonify({"session_id": session_id, "table": table, "data": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@pinces_bp.route("/api/sessions/<session_id>/pince1", methods=["GET"])
def get_pince1(session_id: str):
    return _pince_route(session_id, "pince1_data")


@pinces_bp.route("/api/sessions/<session_id>/pince2", methods=["GET"])
def get_pince2(session_id: str):
    return _pince_route(session_id, "pince2_data")
