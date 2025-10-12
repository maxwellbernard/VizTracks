import base64
import logging
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

from flask import Flask, jsonify, request

app = Flask(__name__)
_level = os.getenv("LOG_LEVEL", "INFO").upper()
app.logger.setLevel(getattr(logging, _level, logging.INFO))
app.logger.info("encoder starting with LOG_LEVEL=%s", _level)

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

    SESSIONS[sid] = {"dir": sdir, "fps": fps, "count": 0, "created": time.time()}
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
    sdir: Path = meta["dir"]
    fps: int = meta["fps"]
    frames: int = meta["count"]

    if frames <= 0:
        _safe_cleanup_session(sid)
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

    app.logger.info("finalizing session %s with %d frames (fps=%d)", sid, frames, fps)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0 or not out_mp4.exists():
            app.logger.error("ffmpeg failed: %s %s", proc.stdout, proc.stderr)
            _safe_cleanup_session(sid)
            return jsonify({"error": "ffmpeg failed"}), 500

        video_b64 = base64.b64encode(out_mp4.read_bytes()).decode("utf-8")
        duration = frames / float(fps)
        payload = {
            "video": video_b64,
            "frames": frames,
            "duration_s": round(duration, 3),
        }

        if CLEANUP_ON_FINALIZE:
            _safe_cleanup_session(sid)
        return jsonify(payload)
    except Exception as e:
        app.logger.exception("finalize failed for session %s: %s", sid, e)
        _safe_cleanup_session(sid)
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
