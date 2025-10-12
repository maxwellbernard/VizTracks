import base64
import io
import logging
import os
import queue
import threading
import time
from typing import Iterator, Optional

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import requests
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from PIL import Image
from requests.adapters import HTTPAdapter, Retry

from backend.core.config import ENCODER_URL

logger = logging.getLogger(__name__)


def _iter_frames_jpeg(anim, facecolor: str = "#F0F0F0") -> Iterator[bytes]:
    """Yield JPEG bytes frame-by-frame without materializing the whole animation."""
    fig = anim._fig
    # Speed-focused rendering settings
    try:
        dpi = int(os.getenv("OUTPUT_DPI", "72"))
    except Exception:
        dpi = 72
    try:
        out_w = int(os.getenv("OUTPUT_WIDTH", "0"))
        out_h = int(os.getenv("OUTPUT_HEIGHT", "0"))
    except Exception:
        out_w = out_h = 0

    try:
        mpl.rcParams["text.antialiased"] = False
        mpl.rcParams["patch.antialiased"] = False
        mpl.rcParams["lines.antialiased"] = False
        mpl.rcParams["agg.path.chunksize"] = 10000
    except Exception:
        pass

    try:
        fig.set_tight_layout(False)  # avoid layout passes
        fig.set_dpi(dpi)
        if out_w > 0 and out_h > 0:
            fig.set_size_inches(out_w / dpi, out_h / dpi, forward=True)
    except Exception:
        pass

    # Choose renderer
    renderer = os.getenv("RENDERER", "savefig").lower()
    canvas = None
    if renderer != "savefig":
        # Ensure Agg canvas and pre-create it for fast buffer access
        try:
            canvas = FigureCanvas(fig)
        except Exception:
            canvas = getattr(fig, "canvas", None)
    if hasattr(anim, "_init_draw"):
        anim._init_draw()

    def _save() -> bytes:
        jpeg_quality = int(os.getenv("JPEG_QUALITY", "75"))
        if renderer == "savefig" or canvas is None:
            # Use savefig (fast in your env) with lean JPEG options
            out = io.BytesIO()
            fig.savefig(
                out,
                format="jpg",
                facecolor=facecolor,
                dpi=fig.dpi,
                pil_kwargs={
                    "quality": jpeg_quality,
                },
            )
            return out.getvalue()
        else:
            # Draw current frame to Agg buffer, then encode to JPEG via Pillow
            canvas.draw()
            w, h = canvas.get_width_height()
            buf = np.frombuffer(canvas.buffer_rgba(), dtype=np.uint8).reshape(h, w, 4)
            img = Image.fromarray(buf[:, :, :3], mode="RGB")
            out = io.BytesIO()
            img.save(
                out, format="JPEG", quality=jpeg_quality, subsampling=2, optimize=False
            )
            return out.getvalue()

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
            # Overlap rendering and uploads using a queue + uploader thread
            batch_size = int(os.getenv("APPEND_BATCH_SIZE", "120"))
            flush_secs = float(os.getenv("UPLOAD_FLUSH_SECS", "0.75"))
            min_flush = int(os.getenv("MIN_UPLOAD_BATCH", "30"))
            frame_queue: "queue.Queue[Optional[bytes]]" = queue.Queue(
                maxsize=batch_size * 3
            )
            send_done = threading.Event()
            send_err: list[Exception] = []

            def uploader() -> None:
                try:
                    sent_local = 0
                    with _session() as up_sess:
                        batch_local: list[str] = []
                        last_flush = time.time()
                        while True:
                            item = frame_queue.get()
                            if item is None:
                                # flush remaining
                                if batch_local:
                                    rr = up_sess.post(
                                        f"{base}/append",
                                        json={
                                            "session_id": session_id,
                                            "frames": batch_local,
                                        },
                                        timeout=180,
                                    )
                                    rr.raise_for_status()
                                    sent_local += len(batch_local)
                                    logger.info(
                                        "client: appended final batch=%s total_sent=%s session=%s",
                                        len(batch_local),
                                        sent_local,
                                        session_id,
                                    )
                                frame_queue.task_done()
                                break
                            # normal frame
                            batch_local.append(base64.b64encode(item).decode("utf-8"))
                            now = time.time()
                            should_time_flush = (now - last_flush) >= flush_secs
                            # Only time-flush if we have a reasonable batch or nothing else is queued
                            if len(batch_local) >= batch_size or (
                                should_time_flush
                                and (
                                    len(batch_local) >= min_flush or frame_queue.empty()
                                )
                            ):
                                rr = up_sess.post(
                                    f"{base}/append",
                                    json={
                                        "session_id": session_id,
                                        "frames": batch_local,
                                    },
                                    timeout=180,
                                )
                                rr.raise_for_status()
                                sent_local += len(batch_local)
                                logger.info(
                                    "client: appended batch=%s total_sent=%s session=%s",
                                    len(batch_local),
                                    sent_local,
                                    session_id,
                                )
                                batch_local.clear()
                                last_flush = now
                            frame_queue.task_done()
                except Exception as ex:
                    send_err.append(ex)
                finally:
                    send_done.set()

            th = threading.Thread(target=uploader, name="uploader", daemon=True)
            th.start()

            # Producer: render frames and feed queue
            for jpg_bytes in _iter_frames_jpeg(anim, facecolor="#F0F0F0"):
                if send_err:
                    raise send_err[0]
                frame_queue.put(jpg_bytes)

            # Signal completion and wait
            frame_queue.put(None)
            th.join()
            if send_err:
                raise send_err[0]

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
