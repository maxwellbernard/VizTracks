from src.visuals.anims.create_bar_animation import create_bar_animation
from src.visuals.plots.create_bar_plot import plot_final_frame


def plot_final_frame_wrapper(
    df,
    top_n,
    analysis_metric,
    selected_attribute,
    start_date,
    end_date,
    image_cache,
    error_logged,
):
    """Thin wrapper around ``plot_final_frame`` with fixed period/days.

    Returns a Matplotlib figure/axes for the last frame of the selected window.

    Args:
        df (pandas.DataFrame): Aggregated data for plotting.
        top_n (int): Number of bars to show.
        analysis_metric (str): Metric to plot.
        selected_attribute (str): artist_name/track_name/album_name.
        start_date, end_date: Date bounds for labels.
        image_cache: LRU/cache for album/artist images.
        error_logged: Shared flag to avoid duplicate error logs.

    Returns:
        matplotlib.figure.Figure: The generated figure.
    """
    return plot_final_frame(
        df=df,
        top_n=top_n,
        analysis_metric=analysis_metric,
        selected_attribute=selected_attribute,
        start_date=start_date,
        end_date=end_date,
        period="M",
        days=30,
        image_cache=image_cache,
        error_logged=error_logged,
    )


def create_bar_animation_wrapper(
    df,
    top_n,
    analysis_metric,
    selected_attribute,
    period,
    dpi,
    days,
    interp_steps,
    start_date,
    end_date,
    figsize,
):
    """Thin wrapper for ``create_bar_animation`` passing through parameters.

    Args:
        df (pandas.DataFrame): Long-form time series per entity.
        top_n (int): Bars to display per frame.
        analysis_metric (str): "Streams" or "duration_ms".
        selected_attribute (str): artist_name/track_name/album_name.
        period (str): Resample period for frames.
        dpi (int): Render DPI; affects resolution.
        days (int): Interpolation window in days.
        interp_steps (int): Interpolation steps between points.
        start_date, end_date: Date bounds for labels.
        figsize (tuple[float, float]): Figure size in inches.

    Returns:
        matplotlib.animation.FuncAnimation: The configured animation.
    """
    return create_bar_animation(
        df,
        top_n,
        analysis_metric,
        selected_attribute,
        period,
        dpi,
        days,
        interp_steps,
        start_date,
        end_date,
        figsize,
    )
