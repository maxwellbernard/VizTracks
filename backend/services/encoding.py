import base64
import io
import os
import tempfile
import time
from typing import Iterator

import matplotlib.pyplot as plt
import requests
from requests import exceptions as req_exc

from backend.core.config import ENCODER_URL


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
    try:
        if not wait_for_ready(base):
            print("[WARN] Encoder health did not become ready before deadline")
            return False
        time.sleep(2)

        url = base + f"/encode_pipe?fps={int(fps)}"
        headers = {
            "Content-Type": "application/octet-stream",
        }

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

        def do_post_file():
            with open(tmp_path, "rb") as f:
                return requests.post(
                    url,
                    data=f,
                    headers=headers,
                    timeout=1200,
                )

        try:
            resp = do_post_file()
            resp.raise_for_status()
        except (req_exc.ConnectionError, req_exc.Timeout, req_exc.HTTPError) as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            # One safe retry on cold-start/proxy errors
            if status in (502, 503, 504) or isinstance(
                e, (req_exc.ConnectionError, req_exc.Timeout)
            ):
                time.sleep(2)
                if not wait_for_ready(base, deadline_sec=30):
                    print("[WARN] Encoder not ready on retry window")
                    return False
                time.sleep(2)
                resp = do_post_file()
                resp.raise_for_status()
            else:
                raise

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
            if "tmp_path" in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def encode_animation(anim, out_path: str, fps: int) -> None:
    """Encode using remote GPU encoder only; no CPU fallback."""
    if not ENCODER_URL:
        raise RuntimeError("ENCODER_URL not set; GPU encoder required")
    ok = encode_animation_remote(anim, out_path, fps)
    if ok:
        try:
            plt.close(anim._fig)
        except Exception:
            pass
        return
    raise RuntimeError("Remote GPU encoder failed")
