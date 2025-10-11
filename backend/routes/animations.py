import base64
import gc
import os
import tempfile
import time

import matplotlib.pyplot as plt
import pandas as pd
import psutil
from flask import Blueprint, jsonify, request

from backend.services.db import query_user_duckdb_for_animation
from backend.services.encoding import encode_animation
from backend.services.system import log_mem
from backend.services.visuals import create_bar_animation_wrapper
from src.visuals import image_cache

bp = Blueprint("animations", __name__)


@bp.route("/generate_animation", methods=["POST"])
def generate_animation():
    try:
        t0 = time.time()
        log_mem("Start /generate_animation")
        data = request.get_json()
        session_id = data.get("session_id")
        selected_attribute = data.get("selected_attribute")
        analysis_metric = data.get("analysis_metric")
        top_n = data.get("top_n", 5)
        start_date = pd.to_datetime(data.get("start_date"))
        end_date = pd.to_datetime(data.get("end_date"))
        fps = data.get("speed_for_bar_animation", 28)
        days = data.get("days", 30)
        interp_steps = data.get("interp_steps", 14)
        period = data.get("period", "d")
        dpi = data.get("dpi", 10)
        figsize = data.get("figsize", (16, 21.2))

        t1 = time.time()
        print(f"Time to parse request data: {t1 - t0:.2f} seconds")
        df = query_user_duckdb_for_animation(
            session_id, selected_attribute, analysis_metric, start_date, end_date
        )
        t2 = time.time()
        print(f"Time to query DuckDB for animation: {t2 - t1:.2f} seconds")
        log_mem("After query_user_duckdb_for_animation")
        if df is None:
            return jsonify(
                {
                    "error": "Session expired. Please upload your data again to generate visuals."
                }
            ), 400

        t3 = time.time()
        anim = create_bar_animation_wrapper(
            df,
            top_n,
            analysis_metric,
            selected_attribute,
            period,
            dpi,
            days,
            interp_steps,
            start_date,
            end_date,
            figsize,
        )
        t4 = time.time()
        print(f"Frame generation (matplotlib) time: {t4 - t3:.2f} seconds")
        log_mem("After create_bar_animation")

        t5 = time.time()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_file:
            temp_path = temp_file.name
        encode_animation(anim, temp_path, fps)
        with open(temp_path, "rb") as f:
            video_bytes = f.read()
        os.remove(temp_path)
        t6 = time.time()
        print(f"Encoding (ffmpeg) time: {t6 - t5:.2f} seconds")
        print(f"Total animation time: {t6 - t3:.2f} seconds")

        video_base64 = base64.b64encode(video_bytes).decode("utf-8")
        filename = f"{selected_attribute}_{analysis_metric}_animation.mp4"

        del anim
        image_cache.clear()
        plt.close("all")
        gc.collect()
        gc.collect()
        ffmpeg_procs = [
            p
            for p in psutil.process_iter(["name"])
            if p.info["name"] and "ffmpeg" in p.info["name"]
        ]
        print(
            f"FFMPEG processes running after cleanup: {len(ffmpeg_procs)}", flush=True
        )
        return jsonify({"video": video_base64, "filename": filename}), 200

    except Exception as e:
        import traceback

        print(traceback.format_exc())
        return jsonify({"error": f"Animation generation failed: {str(e)}"}), 500
