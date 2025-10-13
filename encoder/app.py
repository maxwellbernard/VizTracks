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


# Determine available scalers once
def _detect_scalers() -> dict:
    caps = {"scale_npp": False, "scale_cuda": False}
    try:
        out = subprocess.run(
            ["ffmpeg", "-hide_banner", "-filters"], capture_output=True, text=True
        )
        txt = out.stdout.lower()
        caps["scale_npp"] = "scale_npp" in txt
        caps["scale_cuda"] = "scale_cuda" in txt
    except Exception:
        pass
    return caps


DETECTED_SCALERS = _detect_scalers()
app.logger.info("scalers detected: %s", DETECTED_SCALERS)


def _select_scaler(env_pref: Optional[str]) -> str:
    pref = (env_pref or "auto").lower()
    if pref == "npp":
        return (
            "npp"
            if DETECTED_SCALERS.get("scale_npp")
            else ("cuda" if DETECTED_SCALERS.get("scale_cuda") else "cpu")
        )
    if pref == "cuda":
        return (
            "cuda"
            if DETECTED_SCALERS.get("scale_cuda")
            else ("npp" if DETECTED_SCALERS.get("scale_npp") else "cpu")
        )
    if pref == "cpu":
        return "cpu"
    # auto
    if DETECTED_SCALERS.get("scale_npp"):
        return "npp"
    if DETECTED_SCALERS.get("scale_cuda"):
        return "cuda"
    return "cpu"


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

            app.logger.info(
                "finalize: streaming mp4 session=%s bytes=%s",
                sid,
                out_mp4.stat().st_size,
            )

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


@app.post("/encode_raw")
def encode_raw():
    """Accept raw RGB24 frames over HTTP body and encode to MP4 with NVENC.
    Headers:
      X-Width: integer width in pixels
      X-Height: integer height in pixels
      X-Fps: integer frames per second
      X-PixFmt: rgb24 (optional; only rgb24 supported)
    Body: concatenated raw frames (H*W*3 bytes per frame), unknown length allowed.
    Response: video/mp4 (binary), with Content-Length when available.
    """
    if not nvenc_ready_cached():
        return jsonify({"error": "NVENC not ready"}), 503

    try:
        w = int(request.headers.get("X-Width", "0"))
        h = int(request.headers.get("X-Height", "0"))
        fps = int(request.headers.get("X-Fps", "30"))
        pix = (request.headers.get("X-PixFmt", "rgb24")).lower()
        tgt_w = int(request.headers.get("X-Target-Width", "0"))
        tgt_h = int(request.headers.get("X-Target-Height", "0"))
    except Exception:
        return jsonify({"error": "invalid headers"}), 400
    if w <= 0 or h <= 0 or fps <= 0:
        return jsonify({"error": "invalid dimensions/fps"}), 400
    if pix != "rgb24":
        return jsonify({"error": "only rgb24 supported"}), 415

    sdir = _ensure_sessions_dir() / ("raw-" + uuid.uuid4().hex)
    try:
        sdir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    out_mp4 = sdir / "out.mp4"

    # If no target provided in headers, optionally honor encoder env OUTPUT_WIDTH/HEIGHT as defaults
    if tgt_w <= 0 or tgt_h <= 0:
        try:
            env_tw = int(os.getenv("OUTPUT_WIDTH", "0"))
            env_th = int(os.getenv("OUTPUT_HEIGHT", "0"))
            if env_tw > 0 and env_th > 0:
                tgt_w, tgt_h = env_tw, env_th
                app.logger.info(
                    "encode_raw: using encoder default target %dx%d from env",
                    tgt_w,
                    tgt_h,
                )
        except Exception:
            pass

    # Enforce even dimensions for yuv420p
    if "tgt_w" in locals() and tgt_w > 0 and (tgt_w % 2 == 1):
        tgt_w += 1
    if "tgt_h" in locals() and tgt_h > 0 and (tgt_h % 2 == 1):
        tgt_h += 1

    # Build ffmpeg command: stdin rawvideo → optional scale → MP4 NVENC file
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "rgb24",
        "-s",
        f"{w}x{h}",
        "-r",
        str(fps),
        "-i",
        "pipe:0",
    ]

    # Insert scaler if target size requested and different
    use_scale = (
        "tgt_w" in locals()
        and "tgt_h" in locals()
        and tgt_w > 0
        and tgt_h > 0
        and (tgt_w != w or tgt_h != h)
    )
    if use_scale:
        scaler_choice = _select_scaler(os.getenv("FFMPEG_SCALER"))
        app.logger.info(
            "encode_raw: scaler requested (env=%s) -> %s",
            os.getenv("FFMPEG_SCALER"),
            scaler_choice,
        )
        # Compute aspect-preserving fit inside target, with optional padding to exact target
        try:
            s = min(tgt_w / float(w), tgt_h / float(h))
        except Exception:
            s = 1.0
        fit_w = max(2, int(w * s))
        fit_h = max(2, int(h * s))
        if fit_w % 2:
            fit_w -= 1
        if fit_h % 2:
            fit_h -= 1
        fit_w = min(fit_w, tgt_w - (tgt_w % 2))
        fit_h = min(fit_h, tgt_h - (tgt_h % 2))
        # Determine if aspect matches target closely (avoid padding when true)
        try:
            same_aspect = abs((tgt_w * h) - (tgt_h * w)) <= max(tgt_w, tgt_h)
        except Exception:
            same_aspect = False
        app.logger.info(
            "encode_raw: fit %dx%d within %dx%d (input %dx%d) same_aspect=%s",
            fit_w,
            fit_h,
            tgt_w,
            tgt_h,
            w,
            h,
            same_aspect,
        )
        if same_aspect:
            # Direct scale to the exact target size (no padding needed)
            if scaler_choice == "npp":
                vf = (
                    f"format=nv12,hwupload_cuda,"
                    f"scale_npp={tgt_w}:{tgt_h}:interp_algo=lanczos:format=nv12,"
                    f"setsar=1"
                )
                cmd += ["-vf", vf]
            elif scaler_choice == "cuda":
                vf = (
                    f"format=nv12,hwupload_cuda,"
                    f"scale_cuda={tgt_w}:{tgt_h}:format=nv12,"
                    f"setsar=1"
                )
                cmd += ["-vf", vf]
            else:
                vf = f"scale={tgt_w}:{tgt_h}:flags=lanczos,setsar=1"
                cmd += ["-vf", vf]
        else:
            # Scale to fit and pad to the exact target
            if scaler_choice == "npp":
                vf = (
                    f"format=nv12,hwupload_cuda,"
                    f"scale_npp={fit_w}:{fit_h}:interp_algo=lanczos:format=nv12,"
                    f"hwdownload,format=nv12,"
                    f"pad={tgt_w}:{tgt_h}:(ow-iw)/2:(oh-ih)/2,setsar=1"
                )
                cmd += ["-vf", vf]
            elif scaler_choice == "cuda":
                vf = (
                    f"format=nv12,hwupload_cuda,"
                    f"scale_cuda={fit_w}:{fit_h}:format=nv12,"
                    f"hwdownload,format=nv12,"
                    f"pad={tgt_w}:{tgt_h}:(ow-iw)/2:(oh-ih)/2,setsar=1"
                )
                cmd += ["-vf", vf]
            else:
                vf = (
                    f"scale={fit_w}:{fit_h}:flags=lanczos,"
                    f"pad={tgt_w}:{tgt_h}:(ow-iw)/2:(oh-ih)/2,setsar=1"
                )
                cmd += ["-vf", vf]

    enc_args = get_ffmpeg_args(fps)
    # If using GPU scaler, avoid forcing SW pix_fmt yuv420p which inserts auto_scale
    if use_scale:
        scaler_choice = _select_scaler(os.getenv("FFMPEG_SCALER"))
        if scaler_choice in ("npp", "cuda"):
            try:
                i = 0
                while i < len(enc_args):
                    if (
                        enc_args[i] == "-pix_fmt"
                        and i + 1 < len(enc_args)
                        and enc_args[i + 1] == "yuv420p"
                    ):
                        del enc_args[i : i + 2]
                        break
                    i += 1
            except Exception:
                pass
    cmd += [*enc_args, str(out_mp4)]

    app.logger.info("encode_raw: starting ffmpeg %s", " ".join(cmd))

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
    except Exception as e:
        app.logger.exception("encode_raw: failed to start ffmpeg: %s", e)
        return jsonify({"error": "ffmpeg start failed"}), 500

    # Stream request body into ffmpeg stdin
    bytes_in = 0
    try:
        stream = request.stream
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            bytes_in += len(chunk)
            try:
                proc.stdin.write(chunk)  # type: ignore
            except BrokenPipeError:
                break
    except Exception as e:
        app.logger.exception("encode_raw: read/write error: %s", e)
    finally:
        try:
            if proc.stdin:
                proc.stdin.close()
        except Exception:
            pass

    rc = proc.wait(timeout=1800)
    if rc != 0 or not out_mp4.exists():
        try:
            out = (
                proc.stdout.read().decode("utf-8", errors="ignore")
                if proc.stdout
                else ""
            )
            err = (
                proc.stderr.read().decode("utf-8", errors="ignore")
                if proc.stderr
                else ""
            )
        except Exception:
            out = err = ""
        app.logger.error(
            "encode_raw: ffmpeg failed rc=%s in_bytes=%s stdout=%s stderr=%s",
            rc,
            bytes_in,
            out,
            err,
        )
        return jsonify({"error": "ffmpeg failed"}), 500

    app.logger.info(
        "encode_raw: ok bytes_in=%s out_bytes=%s", bytes_in, out_mp4.stat().st_size
    )
    # Probe encoded dimensions for client logging
    enc_w = enc_h = None
    try:
        probe = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height",
                "-of",
                "csv=p=0",
                str(out_mp4),
            ],
            capture_output=True,
            text=True,
        )
        if probe.returncode == 0 and probe.stdout:
            parts = probe.stdout.strip().split(",")
            if len(parts) >= 2:
                enc_w, enc_h = parts[0], parts[1]
    except Exception:
        pass

    def _iter_out():
        with open(out_mp4, "rb") as f:
            while True:
                b = f.read(1024 * 1024)
                if not b:
                    break
                yield b

    resp = Response(_iter_out(), mimetype="video/mp4")
    try:
        resp.headers["Content-Length"] = str(out_mp4.stat().st_size)
        if enc_w and enc_h:
            resp.headers["X-Encoded-Width"] = str(enc_w)
            resp.headers["X-Encoded-Height"] = str(enc_h)
    except Exception:
        pass
    resp.headers["Connection"] = "close"
    return resp


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
