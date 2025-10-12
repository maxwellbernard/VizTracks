import base64
import io
import logging
import time
from typing import Iterator, Optional

import matplotlib.pyplot as plt
import requests
from requests.adapters import HTTPAdapter, Retry

from backend.core.config import ENCODER_URL

logger = logging.getLogger(__name__)


def _iter_frames_jpeg(anim, facecolor: str = "#F0F0F0") -> Iterator[bytes]:
    """Yield JPEG bytes frame-by-frame without materializing the whole animation."""
    fig = anim._fig
    if hasattr(anim, "_init_draw"):
        anim._init_draw()

    def _save() -> bytes:
        buf = io.BytesIO()
        fig.savefig(
            buf,
            format="jpg",
            facecolor=facecolor,
            dpi=fig.dpi,
            pil_kwargs={"quality": 90, "optimize": True, "progressive": True},
        )
        return buf.getvalue()

    frame_idx = 0
    if hasattr(anim, "new_frame_seq"):
        for framedata in anim.new_frame_seq():
            anim._draw_frame(framedata)
            b = _save()
            if frame_idx % 200 == 0:
                logger.info("client: prepared frame %s (batching)", frame_idx)
            frame_idx += 1
            yield b
    else:
        while True:
            try:
                anim._draw_next_frame(frame_idx, blit=False)
            except StopIteration:
                break
            b = _save()
            if frame_idx % 200 == 0:
                logger.info("client: prepared frame %s (batching)", frame_idx)
            frame_idx += 1
            yield b


def _session() -> requests.Session:
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
    logger.info(
        "client: HTTP session with retries total=%s backoff=%.2f",
        retries.total,
        retries.backoff_factor,
    )
    return s


def _encode_remote(anim, out_path: str, fps: int) -> bool:
    """
    Remote GPU encode by streaming JPEG frames in batches.
    Server is expected to support /start → /append → /finalize and read *.jpg.
    """
    if not ENCODER_URL:
        return False

    base = ENCODER_URL.rstrip("/")
    try:
        t0 = time.perf_counter()
        with _session() as sess:
            # Start session
            r = sess.post(f"{base}/start", json={"fps": fps}, timeout=30)
            r.raise_for_status()
            session_id = r.json().get("session_id")
            if not session_id:
                return False
            logger.info("client: started session id=%s fps=%s", session_id, fps)
            batch: list[str] = []
            batch_size = 60
            sent = 0

            for jpg_bytes in _iter_frames_jpeg(anim, facecolor="#F0F0F0"):
                batch.append(base64.b64encode(jpg_bytes).decode("utf-8"))
                if len(batch) >= batch_size:
                    r = sess.post(
                        f"{base}/append",
                        json={"session_id": session_id, "frames": batch},
                        timeout=120,
                    )
                    r.raise_for_status()
                    sent += len(batch)
                    logger.info(
                        "client: appended batch=%s total_sent=%s session=%s",
                        len(batch),
                        sent,
                        session_id,
                    )
                    batch.clear()

            if batch:
                r = sess.post(
                    f"{base}/append",
                    json={"session_id": session_id, "frames": batch},
                    timeout=120,
                )
                r.raise_for_status()
                sent += len(batch)
                logger.info(
                    "client: appended final batch=%s total_sent=%s session=%s",
                    len(batch),
                    sent,
                    session_id,
                )

            # Finalize and stream file
            logger.info("client: finalizing session=%s", session_id)
            # Disable retries for finalize by using a one-off request without the session's retry adapter
            fin = requests.post(
                f"{base}/finalize",
                json={"session_id": session_id},
                timeout=600,
                stream=True,
            )
            fin.raise_for_status()
            total = 0
            with open(out_path, "wb") as f:
                for chunk in fin.iter_content(chunk_size=1024 * 1024):  # 1 MiB chunks
                    if chunk:
                        f.write(chunk)
                        total += len(chunk)
            logger.info(
                "client: finalize ok bytes=%.2f MiB session=%s",
                total / (1024 * 1024),
                session_id,
            )
            if t0 is not None:
                logger.info(
                    "client: total remote encode time=%.2fs",
                    (time.perf_counter() - t0),
                )
            return True

    except Exception as e:
        logger.exception("client: remote encoder failed: %s", e)
        return False


def encode_animation(anim, out_path: str, fps: int) -> None:
    """
    Remote GPU encoder only (no CPU fallback).
    Raises on failure.
    """
    fig: Optional[plt.Figure] = getattr(anim, "_fig", None)
    try:
        logger.info("client: encode_animation start fps=%s out=%s", fps, out_path)
        ok = _encode_remote(anim, out_path, fps)
        if not ok:
            raise RuntimeError("Remote GPU encoder failed or ENCODER_URL not set")
    finally:
        try:
            if fig is not None:
                plt.close(fig)
        except Exception:
            pass
        logger.info("client: encode_animation end -> %s", out_path)
