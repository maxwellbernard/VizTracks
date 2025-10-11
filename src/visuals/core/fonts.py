"""Font helpers for visuals."""

import os

from matplotlib.font_manager import FontProperties


def get_fonts() -> tuple[FontProperties, FontProperties]:
    """Load custom fonts used across plots.

    Returns:
        tuple[FontProperties, FontProperties]: (heading_font, label_font)
        configured to use Montserrat faces bundled under assets/fonts.
    """
    font_path_heading = os.path.join(
        os.getcwd(), "assets", "fonts", "Montserrat-Bold.ttf"
    )
    font_path_labels = os.path.join(
        os.getcwd(), "assets", "fonts", "Montserrat-SemiBold.ttf"
    )

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
