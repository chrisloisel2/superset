from flask import Blueprint, jsonify, request
import hive_client

tracker_bp = Blueprint("tracker", __name__)


@tracker_bp.route("/api/sessions/<session_id>/tracker", methods=["GET"])
def get_tracker_data(session_id: str):
    limit       = int(request.args.get("limit", 100))
    offset      = int(request.args.get("offset", 0))
    tracker_num = request.args.get("tracker_num")
    limit       = max(1, min(limit, 100))
    offset      = max(0, offset)

    if tracker_num in ("1", "2", "3"):
        n = tracker_num
        cols = f"""
            `timestamp`, time_seconds, frame_number,
            tracker_{n}_x  AS x,
            tracker_{n}_y  AS y,
            tracker_{n}_z  AS z,
            tracker_{n}_qw AS qw,
            tracker_{n}_qx AS qx,
            tracker_{n}_qy AS qy,
            tracker_{n}_qz AS qz
        """
    else:
        cols = "*"

    sql = f"""
        SELECT {cols}
        FROM robotics.tracker_positions
        WHERE session_id = '{session_id}'
        LIMIT {limit} OFFSET {offset}
    """
    try:
        rows = hive_client.run_query(sql)
        return jsonify({"session_id": session_id, "data": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
