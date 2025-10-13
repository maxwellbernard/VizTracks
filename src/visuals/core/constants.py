"""Common visualization constants used across modules."""

from PIL import Image

# Animation/layout defaults
days: int = 30
dpi: int = 60
figsize: tuple[float, float] = (16, 21.2)
interp_steps: int = 17
period: str = "d"

# Resampling filter for image resizing
RESAMPLING_FILTER = Image.Resampling.BILINEAR
