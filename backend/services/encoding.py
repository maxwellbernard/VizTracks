import base64
import io
import logging
import os
import tempfile
import time
from typing import Iterator

import matplotlib.pyplot as plt
import requests
from requests import exceptions as req_exc
from supabase import create_client

from backend.core.config import ENCODER_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL

logger = logging.getLogger(__name__)


def ffmpeg_args_fast(fps: int) -> list[str]:
    """Return ffmpeg arguments optimized for speed and iOS/Safari compatibility.

    Args:
        fps: Frames per second for the animation; used to set GOP size.

    Returns:
        list[str]: Arguments passed to ffmpeg writer (libx264, yuv420p, +faststart,
        ultrafast preset, CRF 30, thread auto, 2-second GOP, no audio).
    """
    return [
        "-vcodec",
        "libx264",
        "-preset",
        "ultrafast",
        "-crf",
        "30",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        "-g",
        str(int(fps * 2)),
        "-sc_threshold",
        "0",
        "-threads",
        "0",
        "-an",
    ]


def _png_stream(anim) -> Iterator[bytes]:
    """Yield PNG bytes for each frame in order without buffering all frames."""
    total_frames = getattr(anim, "total_frames", None)
    if total_frames is None:
        return
    fig = anim._fig
    for i in range(total_frames):
        anim._draw_next_frame(i, blit=False)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", facecolor="#F0F0F0", dpi=fig.dpi)
        yield buf.getvalue()


def encode_animation_remote(anim, out_path: str, fps: int) -> bool:
    """Try remote encoding by streaming PNG frames via chunked upload to /encode_pipe.

    Returns True if remote encoding succeeded; False otherwise.
    """
    if not ENCODER_URL:
        return False

    def wait_for_ready(base: str, deadline_sec: int = 90) -> bool:
        start = time.monotonic()
        consecutive_ok = 0
        while time.monotonic() - start < deadline_sec:
            try:
                r = requests.get(f"{base}/health", timeout=5)
                if r.ok:
                    consecutive_ok += 1
                    if consecutive_ok >= 2:
                        return True
                else:
                    consecutive_ok = 0
            except Exception:
                consecutive_ok = 0
            time.sleep(2)
        return False

    base = ENCODER_URL.rstrip("/")
    tmp_path: str | None = None
    try:
        if not wait_for_ready(base):
            print("[WARN] Encoder health did not become ready before deadline")
            return False
        # Small stabilization to avoid proxy replays right after warmup
        time.sleep(8)

        url = base + f"/encode_pipe?fps={int(fps)}"
        headers: dict[str, str] = {"Content-Type": "application/octet-stream"}

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
            fig = getattr(anim, "_fig", None)
            total_frames = getattr(anim, "total_frames", None)
            if fig is None or total_frames is None:
                raise RuntimeError("Invalid animation object")
            for i in range(total_frames):
                anim._draw_next_frame(i, blit=False)
                buf = io.BytesIO()
                fig.savefig(buf, format="png", facecolor="#F0F0F0", dpi=fig.dpi)
                tmp.write(buf.getvalue())
            size = tmp.tell()

        # Let requests compute Content-Length from the file, but also include our own for proxies
        headers["Content-Length"] = str(size)
        # Prefer closing the connection after request to avoid proxy keep-alive edge cases
        headers["Connection"] = "close"
        headers["Accept-Encoding"] = "identity"
        headers["X-Request-ID"] = str(int(time.time() * 1000))

        def do_post_file():
            with open(tmp_path, "rb") as f:
                # Explicit (connect, read) timeouts; large read timeout for encode duration
                return requests.post(url, data=f, headers=headers, timeout=(10, 1200))

        attempts = 4
        resp = None
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                resp = do_post_file()
                resp.raise_for_status()
                last_exc = None
                break
            except (req_exc.ConnectionError, req_exc.Timeout, req_exc.HTTPError) as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                retriable = status in (500, 502, 503, 504) or isinstance(
                    e, (req_exc.ConnectionError, req_exc.Timeout)
                )
                last_exc = e
                if not retriable or attempt == attempts:
                    break
                time.sleep(3)
                if not wait_for_ready(base, deadline_sec=30):
                    print("[WARN] Encoder not ready during retry window")
                time.sleep(5)

        if last_exc:
            if (
                isinstance(last_exc, req_exc.HTTPError)
                and getattr(last_exc, "response", None) is not None
            ):
                try:
                    detail = last_exc.response.text
                except Exception:
                    detail = "<no response body>"
                print(
                    f"[WARN] Remote encoder HTTPError: {last_exc} body={detail[:512]}"
                )
            else:
                print(f"[WARN] Remote encoder error: {last_exc}")
            return False

        assert resp is not None
        data = resp.json()
        video_b64 = data.get("video")
        if not video_b64:
            return False
        with open(out_path, "wb") as f:
            f.write(base64.b64decode(video_b64))
        return True
    except Exception as e:
        print(f"[WARN] Remote encoder failed. Error: {e}")
        return False
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def encode_animation_via_job(
    anim, out_path: str, fps: int, bucket: str = "viztracks"
) -> bool:
    """Alternative: upload PNG bundle to Supabase, trigger /encode_job, download MP4.

    Returns True on success.
    """
    if not ENCODER_URL or not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.warning("job: missing ENCODER_URL or Supabase credentials; aborting")
        return False
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    base = ENCODER_URL.rstrip("/")

    # 1) Build PNG bundle into temp file
    tmp_png_path = None
    try:
        total_frames = getattr(anim, "total_frames", None)
        logger.info(
            "job: start fps=%s bucket=%s total_frames=%s", fps, bucket, total_frames
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_png_path = tmp.name
            fig = getattr(anim, "_fig", None)
            total_frames = getattr(anim, "total_frames", None)
            if fig is None or total_frames is None:
                raise RuntimeError("Invalid animation object")
            count = 0
            for i in range(total_frames):
                anim._draw_next_frame(i, blit=False)
                buf = io.BytesIO()
                fig.savefig(buf, format="png", facecolor="#F0F0F0", dpi=fig.dpi)
                b = buf.getvalue()
                tmp.write(b)
                count += 1

        size_bytes = os.path.getsize(tmp_png_path) if tmp_png_path else 0
        logger.info(
            "job: built PNG bundle frames=%s size=%sB path=%s",
            count,
            size_bytes,
            tmp_png_path,
        )

        # 2) Upload to Supabase Storage
        key = f"jobs/{int(time.time() * 1000)}.pngpipe"
        with open(tmp_png_path, "rb") as f:
            data = f.read()
        logger.info(
            "job: uploading bundle to supabase bucket=%s key=%s size=%sB",
            bucket,
            key,
            len(data),
        )
        res = sb.storage.from_(bucket).upload(
            key, data, {"content-type": "application/octet-stream", "upsert": True}
        )
        if getattr(res, "error", None):
            logger.warning("job: supabase upload error: %s", res.error)
            return False
        input_url = sb.storage.from_(bucket).get_public_url(key)
        logger.info("job: uploaded bundle url=%s", input_url)

        # 3) Trigger encoder job
        out_key = f"viztracks/{int(time.time() * 1000)}.mp4"
        job = {
            "input_url": input_url,
            "fps": int(fps),
            "output_bucket": bucket,
            "output_path": out_key,
        }
        job_url = base + "/encode_job"
        logger.info("job: posting encode job url=%s output=%s", job_url, out_key)
        r = requests.post(job_url, json=job, timeout=(10, 1200))
        r.raise_for_status()
        url = r.json().get("url")
        if not url:
            logger.warning("job: no URL returned from encode job")
            return False
        logger.info("job: encode completed url=%s", url)

        # 4) Download result
        logger.info("job: downloading result to %s", out_path)
        vr = requests.get(url, timeout=120, stream=True)
        vr.raise_for_status()
        with open(out_path, "wb") as f:
            total_written = 0
            for chunk in vr.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    total_written += len(chunk)
        logger.info("job: download complete size=%sB -> %s", total_written, out_path)
        try:
            plt.close(anim._fig)
        except Exception:
            pass
        return True
    except Exception as e:
        logger.exception("job: encode_animation_via_job failed: %s", e)
        return False
    finally:
        try:
            if tmp_png_path and os.path.exists(tmp_png_path):
                os.remove(tmp_png_path)
        except Exception:
            pass


def encode_animation(anim, out_path: str, fps: int) -> None:
    """Encode using remote GPU encoder via streaming; fallback to job workflow."""
    if not ENCODER_URL:
        raise RuntimeError("ENCODER_URL not set; GPU encoder required")
    # ok = encode_animation_remote(anim, out_path, fps)
    ok = encode_animation_via_job(anim, out_path, fps)
    # if not ok:
    #     ok = encode_animation_via_job(anim, out_path, fps)
    if ok:
        try:
            plt.close(anim._fig)
        except Exception:
            pass
        return
    raise RuntimeError("Remote GPU encoder failed")
