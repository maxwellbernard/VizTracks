import base64
import logging
import os
import time
from typing import List

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
_level = os.getenv("LOG_LEVEL", "INFO").upper()
app.logger.setLevel(getattr(logging, _level, logging.INFO))
app.logger.info("encoder starting with LOG_LEVEL=%s", _level)

READY: bool | None = None


def nvenc_ready_cached() -> bool:
    global READY
    if READY is True:
        return True
    try:
        import subprocess

        accels = subprocess.run(
            ["ffmpeg", "-hide_banner", "-hwaccels"], capture_output=True, text=True
        )
        READY = accels.returncode == 0 and ("cuda" in accels.stdout.lower())
    except Exception:
        READY = False
    return bool(READY)


def get_ffmpeg_args(fps: int) -> list[str]:
    return [
        # Encode with NVENC; no hwaccel flags needed for image2pipe PNG input
        "-c:v",
        "h264_nvenc",
        "-preset",
        "p1",
        "-rc",
        "vbr",
        "-tune",
        "hq",
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


def supabase_client():
    from supabase import create_client

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL/KEY not configured")
    return create_client(url, key)


@app.get("/")
def root():
    return "ok", 200


@app.get("/health")
def health():
    if nvenc_ready_cached():
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "starting"}), 503


@app.post("/encode")
def encode():
    if not nvenc_ready_cached():
        return jsonify({"error": "starting"}), 503
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


@app.post("/encode_pipe")
def encode_pipe():
    """Stream PNG bytes directly to ffmpeg via stdin using image2pipe.

    Query params:
      - fps: frames per second
    Body:
      - application/octet-stream with concatenated PNG images in order
    Response:
      - JSON { video: base64 MP4 }
    """
    try:
        if not nvenc_ready_cached():
            return jsonify({"error": "starting"}), 503
        fps = int(request.args.get("fps", 28))
    except Exception:
        fps = 28

    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_out:
        out_path = tmp_out.name
    with tempfile.NamedTemporaryFile(suffix=".pipe", delete=False) as tmp_in:
        in_path = tmp_in.name

    try:
        # Sanity check for NVENC
        accels = subprocess.run(
            ["ffmpeg", "-hide_banner", "-hwaccels"], capture_output=True, text=True
        )
        if accels.returncode != 0 or "cuda" not in accels.stdout.lower():
            return jsonify({"error": "GPU (CUDA/NVENC) not available"}), 500

        # First, buffer the entire request body to a temp file to avoid mid-upload socket issues
        total_bytes = 0
        t0 = time.perf_counter()
        # Use larger chunks to reduce syscall overhead (default 1 MiB; override via CHUNK_SIZE_BYTES, cap at 4 MiB)
        try:
            env_chunk = int(os.getenv("CHUNK_SIZE_BYTES", str(1024 * 1024)))
        except Exception:
            env_chunk = 1024 * 1024
        chunk_size = max(1024 * 1024, min(env_chunk, 4 * 1024 * 1024))
        app.logger.info(f"/encode_pipe using chunk_size={chunk_size} bytes")
        PNG_SIG = b"\x89PNG\r\n\x1a\n"
        sig_len = len(PNG_SIG)
        carry = b""
        frame_count = 0
        with open(in_path, "wb") as w:
            while True:
                chunk = request.stream.read(chunk_size)
                if not chunk:
                    break
                scan_buf = carry + chunk
                idx = 0
                while True:
                    hit = scan_buf.find(PNG_SIG, idx)
                    if hit == -1:
                        break
                    frame_count += 1
                    app.logger.info(
                        f"pipe: wrote frame {frame_count} (chunk={len(chunk)} bytes, total={total_bytes})"
                    )
                    idx = hit + sig_len
                total_bytes += len(chunk)
                w.write(chunk)

        # Now run ffmpeg and feed the buffered bytes through stdin
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
        app.logger.info(f"Starting ffmpeg (pipe): {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        with open(in_path, "rb") as r:
            while True:
                chunk = r.read(chunk_size)
                if not chunk:
                    break
                try:
                    proc.stdin.write(chunk)
                except BrokenPipeError:
                    try:
                        err_txt = proc.stderr.read().decode("utf-8", errors="ignore")
                    except Exception:
                        err_txt = "<stderr read failed>"
                    app.logger.error(
                        "write to ffmpeg stdin failed: Broken pipe; stderr=\n%s",
                        err_txt,
                    )
                    return jsonify(
                        {"error": "ffmpeg broken pipe", "detail": err_txt}
                    ), 500
        try:
            proc.stdin.close()
        except Exception:
            pass

        stdout, stderr = proc.communicate(timeout=1200)
        t1 = time.perf_counter()

        if proc.returncode != 0:
            err_txt = stderr.decode("utf-8", errors="ignore") if stderr else ""
            app.logger.error(
                "ffmpeg (pipe) failed rc=%s bytes=%s err=\n%s",
                proc.returncode,
                total_bytes,
                err_txt,
            )
            return jsonify({"error": "ffmpeg failed", "detail": err_txt}), 500

        app.logger.info(
            f"ffmpeg (pipe) ok in {t1 - t0:.3f}s; received {total_bytes} bytes"
        )
        with open(out_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode("utf-8")
        return jsonify({"video": video_b64}), 200
    except Exception as e:
        app.logger.exception("/encode_pipe error: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            os.remove(out_path)
        except Exception:
            pass
        try:
            os.remove(in_path)
        except Exception:
            pass


@app.post("/encode_job")
def encode_job():
    """Job-style encode to avoid long-lived upload sockets.

    Body JSON:
      - input_url: Supabase public (or signed) URL to concatenated PNGs
      - fps: int
      - output_bucket: Supabase Storage bucket to upload MP4
      - output_path: Path within bucket for the MP4

    Returns: { url: public_url }
    """
    try:
        if not nvenc_ready_cached():
            return jsonify({"error": "starting"}), 503
        data = request.get_json(force=True)
        input_url = data.get("input_url")
        output_bucket = data.get("output_bucket")
        output_path = data.get("output_path")
        fps = int(data.get("fps", 28))
        if not input_url or not output_bucket or not output_path:
            return jsonify({"error": "missing required fields"}), 400

        # Download source into a temp file
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".pngpipe", delete=False) as tmp_in:
            in_path = tmp_in.name
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_out:
            out_path = tmp_out.name

        r = requests.get(input_url, stream=True, timeout=(10, 1200))
        r.raise_for_status()
        total = 0
        with open(in_path, "wb") as w:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    break
                total += len(chunk)
                w.write(chunk)
        app.logger.info(f"downloaded source {total} bytes from {input_url}")

        # Run ffmpeg
        import subprocess

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
        app.logger.info(f"Starting ffmpeg (job): {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        with open(in_path, "rb") as r2:
            while True:
                buf = r2.read(1024 * 1024)
                if not buf:
                    break
                proc.stdin.write(buf)
        proc.stdin.close()
        _stdout, stderr = proc.communicate(timeout=1200)
        if proc.returncode != 0:
            err_txt = stderr.decode("utf-8", errors="ignore") if stderr else ""
            app.logger.error("ffmpeg failed (job): %s", err_txt)
            return jsonify({"error": "ffmpeg failed", "detail": err_txt}), 500

        # Upload to Supabase Storage
        sb = supabase_client()
        with open(out_path, "rb") as f:
            data = f.read()
        res = sb.storage.from_(output_bucket).upload(
            output_path,
            data,
            {
                "content-type": "video/mp4",
                "upsert": True,
            },
        )
        if getattr(res, "error", None):
            return jsonify({"error": str(res.error)}), 500
        public_url = sb.storage.from_(output_bucket).get_public_url(output_path)
        return jsonify({"url": public_url}), 200
    except Exception as e:
        app.logger.exception("/encode_job error: %s", e)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
