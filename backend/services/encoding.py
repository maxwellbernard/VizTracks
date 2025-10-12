import base64
import os
import queue
import threading
import time
from typing import Iterator, Optional, Tuple

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import requests
from requests.adapters import HTTPAdapter, Retry

from backend.core.config import ENCODER_URL

try:
    from turbojpeg import TurboJPEG  # type: ignore

    _JPEG = TurboJPEG()
    _USE_TURBO = True
except Exception:
    _JPEG = None
    _USE_TURBO = False

import logging

log = logging.getLogger("client")


# Tunables
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


TARGET_W = _env_int("OUTPUT_WIDTH", 1280)
TARGET_H = _env_int("OUTPUT_HEIGHT", 720)
TARGET_DPI = _env_int("OUTPUT_DPI", 96)
JPEG_QUALITY = _env_int("JPEG_QUALITY", 80)
JPEG_SUBSAMPLE = _env_int("JPEG_SUBSAMPLE", 1)
BATCH_SIZE = _env_int("APPEND_BATCH_SIZE", 30)
FLUSH_INTERVAL_S = _env_float("UPLOAD_FLUSH_SECS", 1.0)
QUEUE_MAX = _env_int("UPLOAD_QUEUE_MAX", max(16, BATCH_SIZE * 2))


# Helpers
def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def _iter_frames_rgb(
    anim, *, face: str = "#F0F0F0"
) -> Iterator[Tuple[bytes, int, int]]:
    """
    Yield raw RGB bytes (H x W x 3) from Agg without savefig()/Pillow round-trips.
    Returns (rgb_bytes, width, height).
    """
    fig: plt.Figure = anim._fig
    fig.set_size_inches(TARGET_W / TARGET_DPI, TARGET_H / TARGET_DPI)
    fig.set_dpi(TARGET_DPI)
    fig.patch.set_facecolor(face)
    fig.patch.set_alpha(1.0)
    for ax in fig.axes:
        ax.patch.set_alpha(1.0)

    if hasattr(anim, "_init_draw"):
        anim._init_draw()

    canvas = fig.canvas

    def grab() -> Tuple[bytes, int, int]:
        canvas.draw()
        w, h = canvas.get_width_height()

        buf = np.frombuffer(canvas.buffer_rgba(), dtype=np.uint8).reshape((h, w, 4))
        rgb = buf[:, :, :3].copy(order="C")
        return rgb.tobytes(), w, h

    if hasattr(anim, "new_frame_seq"):
        for framedata in anim.new_frame_seq():
            anim._draw_frame(framedata)
            yield grab()
    else:
        i = 0
        while True:
            try:
                anim._draw_next_frame(i, blit=False)
            except StopIteration:
                break
            yield grab()
            i += 1


def _encode_jpeg(rgb_bytes: bytes, w: int, h: int) -> bytes:
    """Encode raw RGB → JPEG bytes using TurboJPEG (preferred) or Pillow fallback."""
    if _USE_TURBO:
        arr = np.frombuffer(rgb_bytes, dtype=np.uint8).reshape((h, w, 3))
        return _JPEG.encode(arr, quality=JPEG_QUALITY, jpeg_subsample=JPEG_SUBSAMPLE)
    else:
        logging.warning("TurboJPEG not available; using slower Pillow fallback")
        from PIL import Image

        im = Image.frombytes("RGB", (w, h), rgb_bytes)
        import io

        buf = io.BytesIO()
        im.save(
            buf, format="JPEG", quality=JPEG_QUALITY, optimize=False, progressive=False
        )
        return buf.getvalue()


def _uploader(
    session: requests.Session,
    base: str,
    sid: str,
    in_q: "queue.Queue[Optional[Tuple[bytes,int,int]]]",
):
    """Background worker: encode to JPEG, base64, and POST /append in batches."""
    append_url = f"{base}/append"
    batch = []
    last_flush = time.monotonic()
    total_sent = 0

    while True:
        item = in_q.get()
        if item is None:
            break

        rgb, w, h = item
        jpg = _encode_jpeg(rgb, w, h)
        batch.append(base64.b64encode(jpg).decode("utf-8"))

        now = time.monotonic()
        if len(batch) >= BATCH_SIZE or (now - last_flush) > FLUSH_INTERVAL_S:
            r = session.post(
                append_url, json={"session_id": sid, "frames": batch}, timeout=120
            )
            if r.status_code >= 400:
                raise requests.HTTPError(f"/append {r.status_code}: {r.text}")
            total_sent += len(batch)
            log.info(
                "client: appended batch=%d total_sent=%d session=%s",
                len(batch),
                total_sent,
                sid,
            )
            batch.clear()
            last_flush = now

    if batch:
        r = session.post(
            append_url, json={"session_id": sid, "frames": batch}, timeout=120
        )
        if r.status_code >= 400:
            raise requests.HTTPError(f"/append {r.status_code}: {r.text}")
        total_sent += len(batch)
        log.info(
            "client: appended final batch=%d total_sent=%d session=%s",
            len(batch),
            total_sent,
            sid,
        )


def _finalize_to_file(sess: requests.Session, base: str, sid: str, out_path: str):
    """Finalize and write MP4. Supports binary or JSON(base64) server responses."""
    fin = sess.post(
        f"{base}/finalize", json={"session_id": sid}, timeout=600, stream=True
    )
    fin.raise_for_status()
    ctype = fin.headers.get("content-type", "")
    if ctype.startswith("video/"):
        size = 0
        with open(out_path, "wb") as f:
            for chunk in fin.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    size += len(chunk)
        mib = size / (1024 * 1024)
        log.info("client: finalize ok bytes=%.2f MiB session=%s", mib, sid)
    else:
        data = fin.json()
        video_b64 = data.get("video")
        if not video_b64:
            raise RuntimeError("finalize returned no video")
        with open(out_path, "wb") as f:
            f.write(base64.b64decode(video_b64))
        mib = len(video_b64) / 1_333_333  # rough base64→MiB
        log.info("client: finalize ok ~bytes=%.2f MiB (b64) session=%s", mib, sid)


def encode_animation(anim, out_path: str, fps: int) -> None:
    """
    Remote GPU encoder only (no CPU fallback).
    Fast path: Agg render → TurboJPEG → batched /append → /finalize (binary).
    """
    if not ENCODER_URL:
        raise RuntimeError("ENCODER_URL not set; GPU encoder required")

    t0 = time.monotonic()
    log.info("client: encode_animation start fps=%s out=%s", fps, out_path)
    base = ENCODER_URL.rstrip("/")

    with _make_session() as sess:
        log.info("client: HTTP session with retries total=5 backoff=0.60")
        r = sess.post(f"{base}/start", json={"fps": fps}, timeout=30)
        r.raise_for_status()
        sid = r.json().get("session_id")
        if not sid:
            raise RuntimeError("remote /start did not return session_id")
        log.info("client: started session id=%s fps=%s", sid, fps)

        q: "queue.Queue[Optional[Tuple[bytes,int,int]]]" = queue.Queue(
            maxsize=QUEUE_MAX
        )
        worker = threading.Thread(
            target=_uploader, args=(sess, base, sid, q), daemon=True
        )
        worker.start()

        for idx, (rgb, w, h) in enumerate(_iter_frames_rgb(anim, face="#F0F0F0")):
            if idx % 200 == 0:
                log.info("client: prepared frame %d (batching)", idx)
            q.put((rgb, w, h))

        q.put(None)
        worker.join()

        log.info("client: finalizing session=%s", sid)
        _finalize_to_file(sess, base, sid, out_path)
        t1 = time.monotonic()

    log.info("client: total remote encode time=%.2fs", (t1 - t0))
    log.info("client: encode_animation end -> %s", out_path)
