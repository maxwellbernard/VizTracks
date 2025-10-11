import base64
import logging
import os
import time
from typing import List

from flask import Flask, jsonify, request

app = Flask(__name__)
_level = os.getenv("LOG_LEVEL", "INFO").upper()
app.logger.setLevel(getattr(logging, _level, logging.INFO))


def get_ffmpeg_args(fps: int) -> list[str]:
    return [
        "-hwaccel",
        "cuda",
        "-hwaccel_output_format",
        "cuda",
        "-c:v",
        "h264_nvenc",
        "-preset",
        "p1",
        "-rc",
        "vbr_hq",
        "-cq",
        "30",
        "-b:v",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-g",
        str(int(fps * 2)),
        "-movflags",
        "+faststart",
        "-threads",
        "0",
        "-an",
    ]


@app.get("/")
def root():
    return "ok", 200


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/encode")
def encode():
    data = request.get_json(force=True)
    fps = int(data.get("fps", 28))
    frames_b64: List[str] = data.get("frames", [])
    if not frames_b64:
        return jsonify({"error": "frames required"}), 400

    # ffmpeg pipe writing a sequence of PNG images
    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_out:
        out_path = tmp_out.name

    try:
        import subprocess

        accels = subprocess.run(
            ["ffmpeg", "-hide_banner", "-hwaccels"], capture_output=True, text=True
        )
        if accels.returncode != 0 or "cuda" not in accels.stdout.lower():
            return jsonify(
                {"error": "GPU (CUDA/NVENC) not available on this machine"}
            ), 500
        app.logger.info(f"/encode requested: fps={fps}, frames={len(frames_b64)}")
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "image2pipe",
            "-vcodec",
            "png",
            "-framerate",
            str(fps),
            "-i",
            "-",
            *get_ffmpeg_args(fps),
            out_path,
        ]
        app.logger.info(f"Starting ffmpeg: {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        feed_start = time.perf_counter()
        total_bytes = 0
        for idx, b64 in enumerate(frames_b64, start=1):
            png_bytes = base64.b64decode(b64)
            total_bytes += len(png_bytes)
            proc.stdin.write(png_bytes)
            # Per-frame progress log
            app.logger.info(
                f"wrote frame {idx}/{len(frames_b64)} ({len(png_bytes)} bytes)"
            )
        proc.stdin.close()
        feed_end = time.perf_counter()
        app.logger.info(
            f"finished feeding {len(frames_b64)} frames to ffmpeg in {feed_end - feed_start:.3f}s; total_bytes={total_bytes}"
        )
        run_start = time.perf_counter()
        stdout, stderr = proc.communicate(timeout=1200)
        run_end = time.perf_counter()
        if proc.returncode != 0:
            try:
                err_txt = stderr.decode("utf-8", errors="ignore")
            except Exception:
                err_txt = "<stderr decode failed>"
            app.logger.error(
                "ffmpeg failed: returncode=%s\nstderr:\n%s", proc.returncode, err_txt
            )
            return jsonify(
                {
                    "error": "ffmpeg failed",
                    "detail": err_txt,
                }
            ), 500
        with open(out_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode("utf-8")
        app.logger.info(
            f"ffmpeg succeeded in {run_end - run_start:.3f}s; total end-to-end: {(run_end - feed_start):.3f}s"
        )
        return jsonify({"video": video_b64}), 200
    except Exception as e:
        app.logger.exception("/encode error: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            os.remove(out_path)
        except Exception:
            pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
