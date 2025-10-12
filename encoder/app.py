import base64
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

from flask import Flask, Response, jsonify, request

app = Flask(__name__)
_level = os.getenv("LOG_LEVEL", "INFO").upper()
app.logger.setLevel(getattr(logging, _level, logging.INFO))
app.logger.info("encoder starting with LOG_LEVEL=%s", _level)
app.config["USE_X_SENDFILE"] = False

READY: Optional[bool] = None

# Session state: session_id -> {"dir": Path, "fps": int, "count": int, "created": float}
SESSIONS: Dict[str, Dict] = {}

# Limits
MAX_FRAMES_PER_SESSION = int(os.getenv("MAX_FRAMES_PER_SESSION", "10000"))
MAX_AGE_SECONDS = int(os.getenv("SESSION_MAX_AGE_SECONDS", "3600"))  # 1 hour
CLEANUP_ON_FINALIZE = True
FRAME_EXT = ".jpg"
FFMPEG_INPUT_PATTERN = "%06d.jpg"  # zero-padded sequence (000001.jpg, ...)


def _has_h264_nvenc() -> bool:
    try:
        enc = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"], capture_output=True, text=True
        )
        if enc.returncode != 0:
            return False
        return "h264_nvenc" in enc.stdout
    except Exception:
        return False


def nvenc_ready_cached() -> bool:
    global READY
    if READY is True:
        return True
    try:
        accels = subprocess.run(
            ["ffmpeg", "-hide_banner", "-hwaccels"], capture_output=True, text=True
        )
        READY = (
            accels.returncode == 0
            and ("cuda" in accels.stdout.lower())
            and _has_h264_nvenc()
        )
    except Exception:
        READY = False
    return bool(READY)


def get_ffmpeg_args(fps: int) -> list[str]:
    return [
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


def _ensure_sessions_dir() -> Path:
    base = os.getenv("SESSIONS_BASE_DIR")
    if base:
        p = Path(base)
        p.mkdir(parents=True, exist_ok=True)
        return p
    return Path(tempfile.gettempdir()) / "encoder_sessions"


def _cleanup_old_sessions():
    now = time.time()
    base = _ensure_sessions_dir()
    for sid, meta in list(SESSIONS.items()):
        if now - meta.get("created", now) > MAX_AGE_SECONDS:
            try:
                shutil.rmtree(meta["dir"], ignore_errors=True)
            except Exception:
                pass
            SESSIONS.pop(sid, None)
    if base.exists():
        for d in base.iterdir():
            if d.is_dir():
                if any(meta["dir"] == d for meta in SESSIONS.values()):
                    continue
                try:
                    if (now - d.stat().st_mtime) > MAX_AGE_SECONDS:
                        shutil.rmtree(d, ignore_errors=True)
                except Exception:
                    pass


@app.get("/")
def root():
    return "ok", 200


@app.get("/health")
def health():
    status = "ok" if nvenc_ready_cached() else "starting"
    return jsonify({"status": status}), (200 if status == "ok" else 503)


@app.post("/start")
def start():
    """
    Start a new encoding session.
    Body: {"fps": <int>}
    Returns: {"session_id": "..."}
    """
    if not nvenc_ready_cached():
        return jsonify({"error": "NVENC not ready"}), 503

    data = request.get_json(silent=True) or {}
    fps = int(data.get("fps", 30))
    if fps <= 0 or fps > 240:
        return jsonify({"error": "invalid fps"}), 400

    _cleanup_old_sessions()

    sid = uuid.uuid4().hex
    base = _ensure_sessions_dir()
    sdir = base / sid
    sdir.mkdir(parents=True, exist_ok=True)

    SESSIONS[sid] = {
        "dir": sdir,
        "fps": fps,
        "count": 0,
        "created": time.time(),
        "lock": threading.Lock(),
        "finalizing": False,
        "finalized": False,
    }
    app.logger.info("session %s started (fps=%d, dir=%s)", sid, fps, sdir)
    return jsonify({"session_id": sid})


@app.post("/append")
def append():
    """
    Append frames to a session.
    Body: {"session_id": "...", "frames": ["<b64 JPEG>", ...]}
    Returns: {"ok": true, "added": N, "total": M}
    """
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    frames = data.get("frames") or []

    if not sid or sid not in SESSIONS:
        app.logger.warning("append: invalid session_id=%s", sid)
        return jsonify({"error": "invalid session_id"}), 400
    if not isinstance(frames, list) or not frames:
        app.logger.warning("append: no frames provided session=%s", sid)
        return jsonify({"error": "no frames provided"}), 400
    if len(frames) > 600:
        app.logger.warning("append: too many frames in one batch: %s", len(frames))
        return jsonify({"error": "too many frames in one batch"}), 413

    meta = SESSIONS[sid]
    # Disallow appends while finalizing or after finalized to avoid corruption
    if meta.get("finalizing"):
        app.logger.warning("append: session=%s is finalizing; rejecting batch", sid)
        return jsonify({"error": "session is finalizing"}), 409
    if meta.get("finalized"):
        app.logger.warning("append: session=%s already finalized; rejecting batch", sid)
        return jsonify({"error": "session already finalized"}), 409
    sdir: Path = meta["dir"]
    total = meta["count"]

    if total + len(frames) > MAX_FRAMES_PER_SESSION:
        app.logger.warning(
            "append: frame limit exceeded session=%s total=%d incoming=%d limit=%d",
            sid,
            total,
            len(frames),
            MAX_FRAMES_PER_SESSION,
        )
        return jsonify({"error": "frame limit exceeded"}), 413

    added = 0
    try:
        for b64jpg in frames:
            total += 1
            fname = sdir / f"{total:06d}{FRAME_EXT}"
            with open(fname, "wb") as f:
                f.write(base64.b64decode(b64jpg))
            added += 1
        meta["count"] = total
        app.logger.info(
            "append: session=%s added=%d batch=%d total=%d",
            sid,
            added,
            len(frames),
            total,
        )
        return jsonify({"ok": True, "added": added, "total": total})
    except Exception as e:
        app.logger.exception("append: failed session=%s err=%s", sid, e)
        return jsonify({"error": "append failed"}), 500


@app.post("/finalize")
def finalize():
    """
    Finalize a session and return the encoded video.
    Body: {"session_id": "..."}
    Returns: {"video": "<b64 mp4>", "frames": N, "duration_s": float}
    """
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid or sid not in SESSIONS:
        return jsonify({"error": "invalid session_id"}), 400

    meta = SESSIONS[sid]
    lock: threading.Lock = meta.get("lock")  # type: ignore
    # If already finalized and file exists, return it idempotently (streamed)
    sdir_existing: Path = SESSIONS[sid]["dir"]
    out_existing = sdir_existing / "out.mp4"
    if meta.get("finalized") and out_existing.exists():

        def _iter_existing():
            with open(out_existing, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk

        resp = Response(_iter_existing(), mimetype="video/mp4")
        try:
            resp.headers["Content-Length"] = str(out_existing.stat().st_size)
        except Exception:
            pass
        resp.headers["Connection"] = "close"
        return resp

    # Prevent concurrent appends/finalize
    if not lock:
        lock = threading.Lock()
        meta["lock"] = lock

    with lock:
        if meta.get("finalized") and (Path(meta["dir"]) / "out.mp4").exists():
            out_path = Path(meta["dir"]) / "out.mp4"

            def _iter2():
                with open(out_path, "rb") as f:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        yield chunk

            resp = Response(_iter2(), mimetype="video/mp4")
            try:
                resp.headers["Content-Length"] = str(out_path.stat().st_size)
            except Exception:
                pass
            resp.headers["Connection"] = "close"
            return resp
        if meta.get("finalizing"):
            # Another thread is finalizing; brief wait-loop for up to ~10s
            app.logger.info("finalize: session %s already finalizing; waiting", sid)
            waited = 0.0
            while waited < 10.0 and not meta.get("finalized"):
                time.sleep(0.1)
                waited += 0.1
            if meta.get("finalized") and (Path(meta["dir"]) / "out.mp4").exists():
                out_path = Path(meta["dir"]) / "out.mp4"

                def _iter3():
                    with open(out_path, "rb") as f:
                        while True:
                            chunk = f.read(1024 * 1024)
                            if not chunk:
                                break
                            yield chunk

                resp = Response(_iter3(), mimetype="video/mp4")
                try:
                    resp.headers["Content-Length"] = str(out_path.stat().st_size)
                except Exception:
                    pass
                resp.headers["Connection"] = "close"
                return resp
            # Fallthrough to attempt finalize if still not done

        sdir: Path = meta["dir"]
        fps: int = meta["fps"]
        frames: int = meta["count"]

        if frames <= 0:
            return jsonify({"error": "no frames"}), 400

        out_mp4 = sdir / "out.mp4"

        cmd = [
            "ffmpeg",
            "-loglevel",
            "warning",
            "-y",
            "-framerate",
            str(fps),
            "-i",
            str(sdir / FFMPEG_INPUT_PATTERN),
            *get_ffmpeg_args(fps),
            str(out_mp4),
        ]

        app.logger.info(
            "finalizing session %s with %d frames (fps=%d)", sid, frames, fps
        )
        meta["finalizing"] = True
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0 or not out_mp4.exists():
                app.logger.error("ffmpeg failed: %s %s", proc.stdout, proc.stderr)
                meta["finalizing"] = False
                return jsonify({"error": "ffmpeg failed"}), 500

            # Mark finalized and return the file as binary (streamed); keep session for idempotency
            meta["finalized"] = True

            def _iter_final():
                with open(out_mp4, "rb") as f:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        yield chunk

            resp = Response(_iter_final(), mimetype="video/mp4")
            try:
                resp.headers["Content-Length"] = str(out_mp4.stat().st_size)
                resp.headers["X-Frames"] = str(frames)
                resp.headers["X-Duration-Seconds"] = str(round(frames / float(fps), 3))
            except Exception:
                pass
            resp.headers["Connection"] = "close"
            return resp
        except Exception as e:
            app.logger.exception("finalize failed for session %s: %s", sid, e)
            meta["finalizing"] = False
            return jsonify({"error": "finalize failed"}), 500


def _safe_cleanup_session(sid: str):
    meta = SESSIONS.pop(sid, None)
    if not meta:
        return
    try:
        shutil.rmtree(meta["dir"], ignore_errors=True)
    except Exception:
        pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
