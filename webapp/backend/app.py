from flask import Flask
from flask_cors import CORS
from config import get_cors_allowed_origins
from routes.sessions import sessions_bp
from routes.tracker  import tracker_bp
from routes.pinces   import pinces_bp
from routes.query    import query_bp
from routes.admin    import admin_bp
from routes.metadata import metadata_bp

app = Flask(__name__)
CORS(
    app,
    resources={r"/api/*": {"origins": get_cors_allowed_origins()}},
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
    expose_headers=["Content-Type"],
    supports_credentials=False,
    send_wildcard=True,
    max_age=86400,
)

app.register_blueprint(sessions_bp)
app.register_blueprint(tracker_bp)
app.register_blueprint(pinces_bp)
app.register_blueprint(query_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(metadata_bp)


@app.route("/api/health", methods=["GET"])
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
