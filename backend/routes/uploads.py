import os
import tempfile

from flask import Blueprint, jsonify, request

from backend.core.config import MAX_SESSIONS, UPLOAD_DIR
from backend.services.db import insert_jsons_from_zip_to_duckdb
from backend.services.system import cleanup_old_sessions, log_mem

bp = Blueprint("uploads", __name__)


@bp.route("/process", methods=["POST"])
def process_zip():
    """Ingest a Spotify ZIP upload and create a DuckDB session.

    Args:
        None. Reads the uploaded file from the multipart form field named ``file``.

    Returns:
        flask.Response: JSON with ``session_id``, ``data_min_date``, and ``data_max_date`` on success.
        4xx/5xx with ``error`` message on failure or when the server is busy.
    """
    cleanup_old_sessions()
    # Limit concurrent sessions
    session_files = [f for f in os.listdir(UPLOAD_DIR) if f.endswith(".duckdb")]
    if len(session_files) >= MAX_SESSIONS:
        return jsonify(
            {
                "error": "Server is busy. Too many users are generating visuals right now. Please try again in a few minutes."
            }
        ), 503

    log_mem("Start /process")
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    uploaded_file = request.files["file"]
    if uploaded_file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "uploaded.zip")
            uploaded_file.save(zip_path)
            log_mem("After file save")
            session_id, start_date_file, end_date_file = (
                insert_jsons_from_zip_to_duckdb(zip_path)
            )
            return jsonify(
                {
                    "session_id": session_id,
                    "data_min_date": str(start_date_file),
                    "data_max_date": str(end_date_file),
                }
            ), 200
    except Exception as e:
        log_mem(f"Exception: {str(e)}")
        return jsonify({"error": f"Processing failed: {str(e)}"}), 500
