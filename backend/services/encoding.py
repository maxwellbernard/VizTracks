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


def encode_animation(anim, out_path: str, fps: int) -> None:
    """Encode a Matplotlib animation to MP4 using fast, mobile-safe defaults.

    Args:
        anim: Matplotlib animation object to save.
        out_path: Destination file path ending with .mp4.
        fps: Frames per second for encoding.

    Returns:
        None. Writes the MP4 file to ``out_path``.
    """
    anim.save(
        out_path,
        writer="ffmpeg",
        fps=fps,
        savefig_kwargs={"facecolor": "#F0F0F0"},
        extra_args=ffmpeg_args_fast(fps),
    )
