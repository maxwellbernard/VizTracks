def ffmpeg_args_fast(fps: int) -> list[str]:
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
    anim.save(
        out_path,
        writer="ffmpeg",
        fps=fps,
        savefig_kwargs={"facecolor": "#F0F0F0"},
        extra_args=ffmpeg_args_fast(fps),
    )
