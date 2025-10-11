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
