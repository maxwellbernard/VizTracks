import base64
import io
import logging
import os
import queue
import threading
import time
from typing import Iterator, Optional
from typing import Iterator as TypingIterator

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import requests
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from PIL import Image
from requests.adapters import HTTPAdapter, Retry
from turbojpeg import TJFLAG_FASTDCT, TJPF_RGB, TurboJPEG

from backend.core.config import ENCODER_URL

logger = logging.getLogger(__name__)


def _iter_frames_jpeg(anim, facecolor: str = "#F0F0F0") -> Iterator[bytes]:
    """Yield JPEG bytes frame-by-frame without materializing the whole animation."""
    fig = anim._fig

    try:
        mpl.rcParams["text.antialiased"] = False
        mpl.rcParams["patch.antialiased"] = False
        mpl.rcParams["lines.antialiased"] = False
        mpl.rcParams["agg.path.chunksize"] = 10000
        mpl.rcParams["path.simplify"] = True
    except Exception:
        pass

    try:
        fig.set_tight_layout(False)
    except Exception:
        pass

    renderer = (os.getenv("RENDERER") or "savefig").lower()
    canvas = None
    turbo = None
    if renderer != "savefig":
        try:
            canvas = FigureCanvas(fig)
        except Exception:
            canvas = getattr(fig, "canvas", None)
        try:
            turbo = TurboJPEG()
        except Exception:
            turbo = None
    if hasattr(anim, "_init_draw"):
        anim._init_draw()

    def _save() -> bytes:
        jpeg_quality = int(os.getenv("JPEG_QUALITY", "75"))
        if renderer == "savefig" or canvas is None:
            out = io.BytesIO()
            fig.savefig(
                out,
                format="jpg",
                facecolor=fig.get_facecolor(),
                dpi=fig.dpi,
                pil_kwargs={
                    "quality": jpeg_quality,
                    "optimize": False,
                    "progressive": False,
                },
            )
            return out.getvalue()
        else:
            canvas.draw()
            w, h = canvas.get_width_height()
            buf = np.frombuffer(canvas.buffer_rgba(), dtype=np.uint8).reshape(h, w, 4)
            rgb = buf[:, :, :3].copy(order="C")
            if turbo is not None:
                jpeg_bytes = turbo.encode(
                    rgb,
                    pixel_format=TJPF_RGB,
                    quality=jpeg_quality,
                    jpeg_subsample=2,
                    flags=TJFLAG_FASTDCT,
                )
                return jpeg_bytes
            else:
                logger.warning(
                    "TurboJPEG not available, falling back to Pillow for JPEG encoding"
                )
                img = Image.fromarray(rgb, mode="RGB")
                out = io.BytesIO()
                img.save(
                    out,
                    format="JPEG",
                    quality=jpeg_quality,
                    subsampling=2,
                    optimize=False,
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
                anim._draw_next_frame(frame_idx, blit=True)
            except StopIteration:
                break
            b = _save()
            if frame_idx % 200 == 0:
                logger.info("client: prepared frame %s (batching)", frame_idx)
            frame_idx += 1
            yield b


def _iter_frames_rgb(anim, facecolor: str = "#F0F0F0") -> Iterator[np.ndarray]:
    """Yield contiguous RGB numpy arrays using Agg renderer."""
    fig = anim._fig
    try:
        mpl.rcParams["text.antialiased"] = False
        mpl.rcParams["patch.antialiased"] = False
        mpl.rcParams["lines.antialiased"] = False
        mpl.rcParams["agg.path.chunksize"] = 10000
    except Exception:
        pass
    try:
        fig.set_tight_layout(False)
    except Exception:
        pass
    canvas = FigureCanvas(fig)
    if hasattr(anim, "_init_draw"):
        anim._init_draw()

    frame_idx = 0

    def grab_rgb() -> np.ndarray:
        t0 = time.perf_counter()
        canvas.draw()
        t_draw = time.perf_counter() - t0
        if frame_idx % 100 == 0:
            logger.info("perf: draw %.4f s (frame=%d)", t_draw, frame_idx)
        w, h = canvas.get_width_height()
        buf = np.frombuffer(canvas.buffer_rgba(), dtype=np.uint8).reshape(h, w, 4)
        return buf[:, :, :3].copy(order="C")

    if hasattr(anim, "new_frame_seq"):
        for framedata in anim.new_frame_seq():
            anim._draw_frame(framedata)
            rgb = grab_rgb()
            if frame_idx % 200 == 0:
                logger.info("client: prepared frame %s (rgb)", frame_idx)
            frame_idx += 1
            yield rgb
    else:
        while True:
            try:
                anim._draw_next_frame(frame_idx, blit=True)
            except StopIteration:
                break
            rgb = grab_rgb()
            if frame_idx % 200 == 0:
                logger.info("client: prepared frame %s (rgb)", frame_idx)
            frame_idx += 1
            yield rgb


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
            r = sess.post(f"{base}/start", json={"fps": fps}, timeout=30)
            r.raise_for_status()
            session_id = r.json().get("session_id")
            if not session_id:
                return False
            logger.info("client: started session id=%s fps=%s", session_id, fps)
            batch_size = int(os.getenv("APPEND_BATCH_SIZE", "120"))
            flush_secs = float(os.getenv("UPLOAD_FLUSH_SECS", "0.75"))
            min_flush = int(os.getenv("MIN_UPLOAD_BATCH", "30"))
            send_err: list[Exception] = []

            renderer = os.getenv("RENDERER").lower()
            use_two_stage = renderer == "agg"

            if use_two_stage:
                raw_q: "queue.Queue[Optional[np.ndarray]]" = queue.Queue(
                    maxsize=batch_size * 2
                )
                jpg_q: "queue.Queue[Optional[str]]" = queue.Queue(
                    maxsize=batch_size * 2
                )

                def encoder_worker() -> None:
                    try:
                        try:
                            from turbojpeg import TJFLAG_FASTDCT, TJPF_RGB, TurboJPEG

                            turbo = TurboJPEG()
                        except Exception:
                            turbo = None

                        q = raw_q
                        enc_i = 0
                        while True:
                            item = q.get()
                            if item is None:
                                jpg_q.put(None)
                                q.task_done()
                                break

                            # Support ndarray or (idx, ndarray)
                            if isinstance(item, tuple) and len(item) >= 2:
                                idx, arr = item
                            else:
                                arr = item
                            try:
                                if turbo is not None:
                                    t0 = time.perf_counter()
                                    jpeg_bytes = turbo.encode(
                                        arr,
                                        pixel_format=TJPF_RGB,
                                        quality=int(os.getenv("JPEG_QUALITY", "75")),
                                        jpeg_subsample=2,
                                        flags=TJFLAG_FASTDCT,
                                    )
                                    t_jpeg = time.perf_counter() - t0
                                else:
                                    t0 = time.perf_counter()
                                    img = Image.fromarray(arr, mode="RGB")
                                    out = io.BytesIO()
                                    img.save(
                                        out,
                                        format="JPEG",
                                        quality=int(os.getenv("JPEG_QUALITY", "75")),
                                        subsampling=2,
                                        optimize=False,
                                    )
                                    jpeg_bytes = out.getvalue()
                                    t_jpeg = time.perf_counter() - t0

                                enc_i += 1
                                if (enc_i % 100) == 0:
                                    logger.info(
                                        "client: jpeg %.3f s (frame=%d)", t_jpeg, enc_i
                                    )

                                jpg_q.put(base64.b64encode(jpeg_bytes).decode("utf-8"))
                            finally:
                                q.task_done()
                    except Exception as ex:
                        send_err.append(ex)

                def uploader_worker() -> None:
                    try:
                        sent_local = 0
                        with _session() as up_sess:
                            batch_local: list[str] = []
                            last_flush = time.time()
                            while True:
                                item = jpg_q.get()
                                if item is None:
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
                                    jpg_q.task_done()
                                    break
                                batch_local.append(item)
                                now = time.time()
                                should_time_flush = (now - last_flush) >= flush_secs
                                if len(batch_local) >= batch_size or (
                                    should_time_flush
                                    and (len(batch_local) >= min_flush or jpg_q.empty())
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
                                jpg_q.task_done()
                    except Exception as ex:
                        send_err.append(ex)

                enc_th = threading.Thread(
                    target=encoder_worker, name="jpeg-encoder", daemon=True
                )
                up_th = threading.Thread(
                    target=uploader_worker, name="uploader", daemon=True
                )
                enc_th.start()
                up_th.start()

                for rgb in _iter_frames_rgb(anim, facecolor="#F0F0F0"):
                    if send_err:
                        raise send_err[0]
                    raw_q.put(rgb)
                raw_q.put(None)
                enc_th.join()
                up_th.join()
                if send_err:
                    raise send_err[0]
            else:
                frame_q: "queue.Queue[Optional[bytes]]" = queue.Queue(
                    maxsize=batch_size * 3
                )

                def uploader() -> None:
                    try:
                        sent_local = 0
                        with _session() as up_sess:
                            batch_local: list[str] = []
                            last_flush = time.time()
                            while True:
                                item = frame_q.get()
                                if item is None:
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
                                    frame_q.task_done()
                                    break
                                b64 = base64.b64encode(item).decode("utf-8")
                                batch_local.append(b64)
                                now = time.time()
                                should_time_flush = (now - last_flush) >= flush_secs
                                if len(batch_local) >= batch_size or (
                                    should_time_flush
                                    and (
                                        len(batch_local) >= min_flush or frame_q.empty()
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
                                frame_q.task_done()
                    except Exception as ex:
                        send_err.append(ex)

                up_th = threading.Thread(target=uploader, name="uploader", daemon=True)
                up_th.start()
                for jpg in _iter_frames_jpeg(anim, facecolor="#F0F0F0"):
                    if send_err:
                        raise send_err[0]
                    frame_q.put(jpg)
                frame_q.put(None)
                up_th.join()
                if send_err:
                    raise send_err[0]

            logger.info("client: finalizing session=%s", session_id)
            fin = requests.post(
                f"{base}/finalize",
                json={"session_id": session_id},
                timeout=600,
                stream=True,
            )
            fin.raise_for_status()
            total = 0
            with open(out_path, "wb") as f:
                for chunk in fin.iter_content(chunk_size=1024 * 1024):
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


def _encode_raw(anim, out_path: str, fps: int) -> None:
    """Stream raw RGB frames to the encoder's /encode_raw endpoint and save MP4."""
    if not ENCODER_URL:
        raise RuntimeError("ENCODER_URL not set; GPU encoder required")

    base = ENCODER_URL.rstrip("/")

    def _wait_for_encoder_ready(url_base: str, timeout_s: float = 90.0) -> None:
        deadline = time.monotonic() + timeout_s
        last_err = None
        while time.monotonic() < deadline:
            try:
                r = requests.get(f"{url_base}/health", timeout=3)
                if r.status_code == 200 and (r.json().get("status") == "ok"):
                    return
            except Exception as e:
                last_err = e
            time.sleep(0.5)
        if last_err:
            logger.warning("client: encoder health wait gave up: %s", last_err)
        else:
            logger.warning("client: encoder health wait timed out")

    # Prime the first frame to get dimensions, after waiting for readiness
    _wait_for_encoder_ready(base)
    frame_iter = _iter_frames_rgb(anim, facecolor="#F0F0F0")
    try:
        first = next(frame_iter)
    except StopIteration:
        raise RuntimeError("No frames to encode")

    if not isinstance(first, np.ndarray) or first.ndim != 3 or first.shape[2] != 3:
        raise RuntimeError("Frame must be HxWx3 uint8 array")

    h, w, _ = first.shape
    # Log draw size (Agg canvas size in pixels)
    logger.info("client: draw size %dx%d (figsize*dpi)", w, h)

    def body() -> TypingIterator[bytes]:
        # yield first then the rest
        yield first.tobytes()
        idx = 1
        for arr in frame_iter:
            if idx % 200 == 0:
                logger.info("client: prepared frame %d (raw streaming)", idx)
            # ensure contiguous
            if not arr.flags.c_contiguous:
                arr = np.ascontiguousarray(arr)
            yield arr.tobytes()
            idx += 1

    # Optional upscale target: if OUTPUT_WIDTH/HEIGHT are set, request encoder scaling
    tgt_w = None
    tgt_h = None
    try:
        ow = int(os.getenv("OUTPUT_WIDTH", "0"))
        oh = int(os.getenv("OUTPUT_HEIGHT", "0"))
        if ow > 0 and oh > 0 and (ow != w or oh != h):
            # enforce even dims for yuv420p
            if ow % 2 == 1:
                ow += 1
            if oh % 2 == 1:
                oh += 1
            tgt_w, tgt_h = ow, oh
    except Exception:
        tgt_w = tgt_h = None

    headers = {
        "Content-Type": "application/octet-stream",
        "X-Width": str(w),
        "X-Height": str(h),
        "X-Fps": str(fps),
        "X-PixFmt": "rgb24",
        "Expect": "100-continue",
        "Connection": "close",
    }
    if tgt_w and tgt_h:
        headers["X-Target-Width"] = str(tgt_w)
        headers["X-Target-Height"] = str(tgt_h)

    url = f"{base}/encode_raw"
    logger.info(
        "client: raw encode start %sx%s fps=%s -> %s",
        w,
        h,
        fps,
        out_path,
    )
    with requests.post(
        url, data=body(), headers=headers, stream=True, timeout=1800
    ) as r:
        r.raise_for_status()
        # Log encoded size if encoder provides it via headers
        try:
            enc_w = r.headers.get("X-Encoded-Width")
            enc_h = r.headers.get("X-Encoded-Height")
            if enc_w and enc_h:
                logger.info("client: encoded size %sx%s (from encoder)", enc_w, enc_h)
        except Exception:
            pass
        total = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
        logger.info("client: raw encode ok bytes=%.2f MiB", total / (1024 * 1024))


def encode_animation(anim, out_path: str, fps: int) -> None:
    """
    Remote GPU encoder only (no CPU fallback).
    Raises on failure.
    """
    fig: Optional[plt.Figure] = getattr(anim, "_fig", None)
    try:
        logger.info("client: encode_animation start fps=%s out=%s", fps, out_path)
        # Use raw streaming path by default
        _encode_raw(anim, out_path, fps)
    finally:
        try:
            if fig is not None:
                plt.close(fig)
        except Exception:
            pass
        logger.info("client: encode_animation end -> %s", out_path)
