"""Visuals package public API.
This module re-exports key functions and constants from submodules
to provide a simplified interface.
"""

from .core.cache import color_cache, error_logged, image_cache
from .core.colors import get_dominant_color
from .core.constants import RESAMPLING_FILTER, days, dpi, figsize, interp_steps, period
from .core.fonts import get_fonts
from .core.style import setup_bar_plot_style
from .io.images import fetch_images_batch

__all__ = [
    "days",
    "dpi",
    "figsize",
    "interp_steps",
    "period",
    "RESAMPLING_FILTER",
    "image_cache",
    "color_cache",
    "error_logged",
    "get_dominant_color",
    "get_fonts",
    "setup_bar_plot_style",
    "fetch_images_batch",
]
