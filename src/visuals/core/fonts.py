"""Font helpers for visuals."""

import os

from matplotlib.font_manager import FontProperties


def get_fonts() -> tuple[FontProperties, FontProperties]:
    """Load custom fonts for the plot and return (heading, labels)."""
    font_path_heading = os.path.join(os.getcwd(), "fonts", "Montserrat-Bold.ttf")
    font_path_labels = os.path.join(os.getcwd(), "fonts", "Montserrat-SemiBold.ttf")

    heading = FontProperties(
        family="sans-serif",
        style="normal",
        variant="normal",
        weight="normal",
        stretch="normal",
        size="medium",
        fname=font_path_heading,
    )
    labels = FontProperties(
        family="sans-serif",
        style="normal",
        variant="normal",
        weight="normal",
        stretch="normal",
        size="medium",
        fname=font_path_labels,
    )
    return heading, labels
