from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
from flask import Blueprint, jsonify, request

from backend.services.db import query_user_duckdb
from backend.services.system import cleanup_old_sessions, log_mem
from backend.services.visuals import plot_final_frame_wrapper
from src.data.normalize_inputs import normalize_inputs
from src.visuals import error_logged, image_cache

bp = Blueprint("images", __name__)


@bp.route("/generate_image", methods=["POST"])
def generate_image():
    """Generate a static bar plot image for the requested selection and metric.

    Args:
        None. Reads JSON body with keys: ``session_id``, ``selected_attribute``,
        ``analysis_metric``, ``top_n``, ``start_date``, ``end_date``.

    Returns:
        flask.Response: JSON with Base64-encoded ``image`` and ``filename``.
        4xx/5xx with ``error`` message if the session is missing or processing fails.
    """
    cleanup_old_sessions()
    try:
        log_mem("Start /generate_image")
        data = request.get_json()
        session_id = data.get("session_id")
        selected_attribute = data.get("selected_attribute")
        analysis_metric = data.get("analysis_metric")
        top_n = data.get("top_n", 5)
        start_date = pd.to_datetime(data.get("start_date"))
        end_date = pd.to_datetime(data.get("end_date"))

        # Normalize inputs to internal names
        selected_attribute, analysis_metric = normalize_inputs(
            selected_attribute, analysis_metric
        )

        df = query_user_duckdb(
            session_id, selected_attribute, analysis_metric, start_date, end_date, top_n
        )
        log_mem("After query_user_duckdb")
        if df is None or df.empty:
            return jsonify(
                {
                    "error": "Session expired. Please upload your data again to generate visuals."
                }
            ), 400

        plt.close("all")
        fig = plot_final_frame_wrapper(
            df,
            top_n,
            analysis_metric,
            selected_attribute,
            start_date,
            end_date,
            image_cache,
            error_logged,
        )
        log_mem("After plot_final_frame")

        buf = BytesIO()
        fig.savefig(buf, format="jpeg", dpi=91, facecolor="#F0F0F0", edgecolor="none")
        buf.seek(0)
        import base64

        image_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        plt.close(fig)
        filename = f"{selected_attribute}_{analysis_metric}_visual.jpg"
        return jsonify({"image": image_base64, "filename": filename}), 200

    except Exception as e:
        return jsonify({"error": f"Image generation failed: {str(e)}"}), 500
