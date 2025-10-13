import base64
import gc
import tempfile
import time

import matplotlib.pyplot as plt
import pandas as pd
import psutil
from flask import Blueprint, jsonify, request

from backend.core.config import ENCODER_URL
from backend.services.db import query_user_duckdb_for_animation
from backend.services.encoding import encode_animation
from backend.services.system import log_mem
from backend.services.visuals import create_bar_animation_wrapper
from src.visuals import image_cache

bp = Blueprint("animations", __name__)


@bp.route("/generate_animation", methods=["POST"])
def generate_animation():
    """Generate an MP4 bar-race animation for the given selection and metric.

    Expects a JSON body with:
    - session_id (str): Upload/session identifier.
    - selected_attribute (str): One of artist_name, track_name, album_name.
    - analysis_metric (str): "Streams" or "duration_ms".
    - top_n (int, optional): Number of entities to display. Defaults to 5.
    - start_date (str): ISO date (YYYY-MM-DD) inclusive.
    - end_date (str): ISO date (YYYY-MM-DD) inclusive.
    - speed_for_bar_animation (int, optional): FPS for encoding. Defaults to 28.
    - days (int, optional): Interpolation window size. Defaults to 30.
    - interp_steps (int, optional): Steps between points. Defaults to 14.
    - period (str, optional): Resample period (e.g., 'd', 'M'). Defaults to 'd'.
    - dpi (int, optional): Figure DPI for frames. Defaults to 10.
    - figsize (tuple[float, float], optional): Figure size in inches.

    Returns:
        flask.Response: JSON containing base64-encoded MP4 under key "video"
        and a suggested filename under key "filename". Returns 400 if the session
        is missing/expired, or 500 with an error message on failure.
    """
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
        # Optional render downscale: keep pixel count modest for faster draw
        try:
            import os

            max_w = int(os.getenv("RENDER_MAX_WIDTH", "0"))
            max_h = int(os.getenv("RENDER_MAX_HEIGHT", "0"))
        except Exception:
            max_w = max_h = 0

        eff_dpi = dpi
        upscale_target = None
        try:
            if isinstance(figsize, (list, tuple)) and len(figsize) == 2:
                px_w = float(figsize[0]) * float(dpi)
                px_h = float(figsize[1]) * float(dpi)
                scale = 1.0
                factors = []
                if max_w and px_w > max_w:
                    factors.append(max_w / px_w)
                if max_h and px_h > max_h:
                    factors.append(max_h / px_h)
                if factors:
                    scale = max(min(factors), 0.1)
                eff_dpi = max(1.0, float(dpi) * scale)
                if eff_dpi != dpi:
                    print(
                        f"Render downscale: dpi {dpi} -> {eff_dpi:.2f} (px ~ {px_w:.0f}x{px_h:.0f}) max=({max_w or '-'}x{max_h or '-'})"
                    )
                    # Ask encoder to scale back to the original figure pixel size (no padding)
                    try:
                        ow = int(round(px_w))
                        oh = int(round(px_h))
                        # ensure even for yuv420p
                        if ow % 2 == 1:
                            ow += 1
                        if oh % 2 == 1:
                            oh += 1
                        upscale_target = (ow, oh)
                    except Exception:
                        upscale_target = None
        except Exception:
            pass

        anim = create_bar_animation_wrapper(
            df,
            top_n,
            analysis_metric,
            selected_attribute,
            period,
            eff_dpi,
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
        if ENCODER_URL:
            print(f"Using remote encoder at {ENCODER_URL}")
        encode_animation(anim, temp_path, fps, target=upscale_target)
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

    except RuntimeError as e:
        import traceback

        print(traceback.format_exc())
        return (
            jsonify(
                {
                    "error": f"Animation generation failed due to GPU encoder: {str(e)}",
                }
            ),
            503,
        )
    except Exception as e:
        import traceback

        print(traceback.format_exc())
        return jsonify({"error": f"Animation generation failed: {str(e)}"}), 500
