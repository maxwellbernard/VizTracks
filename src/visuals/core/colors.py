"""Color utilities for visuals."""

from io import BytesIO

from colorthief import ColorThief
from PIL import Image


def get_dominant_color(
    img: Image.Image, img_name: str, cache: dict | None = None
) -> tuple:
    """Extract a vibrant dominant color from an image using ColorThief, avoiding greys.

    Args:
        img: PIL Image to analyze.
        img_name: Unique id for caching purposes.
        cache: Optional dict[str, tuple] cache to reuse results.

    Returns:
        RGB tuple (0-255 each).
    """
    if cache is not None and img_name in cache:
        return cache[img_name]

    with BytesIO() as byte_stream:
        img.save(byte_stream, format="PNG")
        byte_stream.seek(0)
        color_thief = ColorThief(byte_stream)
        palette = color_thief.get_palette(color_count=5, quality=5)

    # Prefer more vibrant colors where possible
    # Fallback to first palette color if nothing matches
    dominant_color = palette[0]
    if cache is not None:
        cache[img_name] = dominant_color
    return dominant_color
