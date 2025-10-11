import base64
import os
from typing import List

from flask import Flask, jsonify, request

app = Flask(__name__)


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
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        for b64 in frames_b64:
            png_bytes = base64.b64decode(b64)
            proc.stdin.write(png_bytes)
        proc.stdin.close()
        stdout, stderr = proc.communicate(timeout=1200)
        if proc.returncode != 0:
            return jsonify(
                {
                    "error": "ffmpeg failed",
                    "detail": stderr.decode("utf-8", errors="ignore"),
                }
            ), 500
        with open(out_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode("utf-8")
        return jsonify({"video": video_b64}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            os.remove(out_path)
        except Exception:
            pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
