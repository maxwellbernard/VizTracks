"""Plot styling helpers for visuals."""

import matplotlib.pyplot as plt


def setup_bar_plot_style(ax: plt.Axes) -> None:
    """Apply consistent styling to a bar chart axes.

    Args:
        ax: Matplotlib Axes to style.

    Returns:
        None. Mutates the provided axes in place.
    """
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_linewidth(3)
    ax.spines["bottom"].set_linewidth(3)
    ax.spines["left"].set_color("grey")
    ax.spines["bottom"].set_color("grey")
    ax.margins(x=0.05)
    ax.set_title(" ", pad=200, fontsize=14, fontweight="bold")
    ax.xaxis.labelpad = 30
    ax.title.set_position([0.5, 1.3])
    ax.title.set_fontsize(20)
    ax.set_facecolor("#F0F0F0")
    ax.set_xticks([])
