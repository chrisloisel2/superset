from flask import Blueprint, jsonify
import hive_client

sessions_bp = Blueprint("sessions", __name__)


@sessions_bp.route("/api/sessions", methods=["GET"])
def list_sessions():
    """List all sessions with frame count and time range."""
    partition_sql = "SHOW PARTITIONS robotics.tracker_positions"
    try:
        part_rows = hive_client.run_query(partition_sql)
        sessions = {}
        if part_rows:
            part_col = next(iter(part_rows[0].keys()))
            for row in part_rows:
                spec = str(row.get(part_col, ""))
                session_id = None
                for piece in spec.split("/"):
                    if piece.startswith("session_id="):
                        session_id = piece.split("=", 1)[1]
                        break
                if session_id:
                    sessions[session_id] = {
                        "session_id": session_id,
                        "frame_count": 0,
                        "start_time": None,
                        "end_time": None,
                    }
        rows = sorted(sessions.values(), key=lambda x: x["session_id"], reverse=True)
        return jsonify({"sessions": rows})
    except Exception as e:
        if "Database does not exist: robotics" in str(e):
            return jsonify({"sessions": []})
        return jsonify({"error": str(e)}), 500


@sessions_bp.route("/api/sessions/<session_id>", methods=["GET"])
def get_session(session_id: str):
    """Summary stats for a single session (tracker + pinces)."""
    tracker_sql = f"""
        SELECT
            COUNT(*)                       AS frame_count,
            MIN(`timestamp`)               AS start_ts,
            MAX(`timestamp`)               AS end_ts,
            MIN(frame_number)              AS first_frame,
            MAX(frame_number)              AS last_frame,
            ROUND(AVG(tracker_1_x), 6)    AS avg_t1_x,
            ROUND(AVG(tracker_1_y), 6)    AS avg_t1_y,
            ROUND(AVG(tracker_1_z), 6)    AS avg_t1_z,
            ROUND(AVG(tracker_2_x), 6)    AS avg_t2_x,
            ROUND(AVG(tracker_2_y), 6)    AS avg_t2_y,
            ROUND(AVG(tracker_2_z), 6)    AS avg_t2_z,
            ROUND(AVG(tracker_3_x), 6)    AS avg_t3_x,
            ROUND(AVG(tracker_3_y), 6)    AS avg_t3_y,
            ROUND(AVG(tracker_3_z), 6)    AS avg_t3_z
        FROM robotics.tracker_positions
        WHERE session_id = '{session_id}'
    """
    pince_sql = f"""
        SELECT
            pince_id,
            COUNT(*)                       AS nb_mesures,
            ROUND(AVG(ouverture_mm), 2)   AS avg_ouverture_mm,
            ROUND(MAX(ouverture_mm), 2)   AS max_ouverture_mm,
            ROUND(AVG(angle_deg), 2)      AS avg_angle_deg
        FROM (
            SELECT pince_id, ouverture_mm, angle_deg
            FROM robotics.pince1_data WHERE session_id = '{session_id}'
            UNION ALL
            SELECT pince_id, ouverture_mm, angle_deg
            FROM robotics.pince2_data WHERE session_id = '{session_id}'
        ) combined
        GROUP BY pince_id
    """
    try:
        tracker_stats = hive_client.run_query(tracker_sql)
        pince_stats   = hive_client.run_query(pince_sql)
        return jsonify({
            "session_id":    session_id,
            "tracker_stats": tracker_stats[0] if tracker_stats else {},
            "pince_stats":   pince_stats,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
