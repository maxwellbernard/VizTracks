import base64
import io

import matplotlib.pyplot as plt
import requests

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


def encode_animation_remote(anim, out_path: str, fps: int) -> bool:
    """Try remote encoding by streaming PNG frames to an encoder service.

    The encoder service accepts a JSON body with base64 PNG frames or a simple
    chunked upload protocol. For simplicity here, we send a json list.

    Returns True if remote encoding succeeded; False otherwise.
    """
    if not ENCODER_URL:
        return False
    try:
        total_frames = getattr(anim, "total_frames", None)
        if total_frames is None:
            return False
        fig = anim._fig  # matplotlib internal but stable enough here
        frames_png: list[str] = []
        # Render each frame to PNG bytes; this is still CPU-heavy but moves video muxing to GPU
        for i in range(total_frames):
            anim._draw_next_frame(i, blit=False)  # advances internal state
            buf = io.BytesIO()
            fig.savefig(buf, format="png", facecolor="#F0F0F0", dpi=fig.dpi)
            frames_png.append(base64.b64encode(buf.getvalue()).decode("utf-8"))
        payload = {
            "fps": fps,
            "frames": frames_png,
        }
        url = ENCODER_URL.rstrip("/") + "/encode"
        resp = requests.post(url, json=payload, timeout=600)
        resp.raise_for_status()
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
