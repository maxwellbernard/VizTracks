"""
This module provides functions to create a bar chart animation for Spotify data analysis.
It includes functions to set up the animation, process data, and handle image fetching and caching.
"""

import os
import textwrap
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

import matplotlib.animation as animation
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from matplotlib.offsetbox import AnnotationBbox, OffsetImage
from PIL import Image

from src.visuals import (
    fetch_images_batch,
    get_dominant_color,
    get_fonts,
    image_cache,
    setup_bar_plot_style,
)
from src.visuals.anims.state import AnimationState
from src.visuals.core.constants import RESAMPLING_FILTER
from src.visuals.core.constants import days as _DEFAULT_DAYS
from src.visuals.core.constants import dpi as _DEFAULT_DPI
from src.visuals.core.constants import figsize as _DEFAULT_FIGSIZE
from src.visuals.core.constants import interp_steps as _DEFAULT_INTERP_STEPS
from src.visuals.core.constants import period as _DEFAULT_PERIOD

# Re-export defaults for external imports (e.g., frontend uses these)
days = _DEFAULT_DAYS
dpi = _DEFAULT_DPI
figsize = _DEFAULT_FIGSIZE
interp_steps = _DEFAULT_INTERP_STEPS
period = _DEFAULT_PERIOD

warnings.filterwarnings(
    "ignore",
    category=UserWarning,
)


def preload_images_batch(
    names, monthly_df, selected_attribute, item_type, top_n, target_size=200
) -> None:
    """Preload images via batch API and parallel downloads.

    Mirrors logic from the static plot to keep caching consistent.
    """
    items_to_fetch = []
    cache_keys = []
    names = list(set(names))
    for name in names:
        cache_key = f"{name}_top_n_{top_n}"
        if selected_attribute == "track_name":
            matching_rows = monthly_df[monthly_df["track_uri"] == name]
        elif selected_attribute == "artist_name":
            matching_rows = monthly_df[monthly_df["artist_name"] == name]
        else:
            matching_rows = monthly_df[monthly_df[selected_attribute] == name]
        if not matching_rows.empty:
            row = matching_rows.iloc[0]
            if selected_attribute == "album_name":
                if "track_uri" in row and row["track_uri"]:
                    cache_key = f"{row['track_uri']}_top_n_{top_n}"
                else:
                    cache_key = f"{name}_album_top_n_{top_n}"
            elif selected_attribute == "track_name":
                if "track_uri" in row and row["track_uri"]:
                    cache_key = f"{row['track_uri']}_top_n_{top_n}"
            elif selected_attribute == "artist_name":
                cache_key = f"{name}_top_n_{top_n}"

        cache_keys.append(cache_key)
        if cache_key in image_cache:
            continue
        else:
            if selected_attribute == "track_name":
                if not matching_rows.empty:
                    row = matching_rows.iloc[0]
                    item_data = {
                        "name": name,
                        "type": "track",
                        "cache_key": cache_key,
                        "track_uri": row["track_uri"],
                    }
                    items_to_fetch.append(item_data)
            elif selected_attribute == "album_name":
                album_name, artist_name = name.split(" - ", 1)
                matching_rows = monthly_df[
                    (monthly_df["album_name"] == album_name)
                    & (monthly_df["artist_name"] == artist_name)
                ]
                if not matching_rows.empty:
                    row = matching_rows.iloc[0]
                    cache_key = f"{name}_album_top_n_{top_n}"
                    item_data = {
                        "name": name,
                        "type": "album",
                        "cache_key": cache_key,
                        "track_uri": row["track_uri"],
                    }
                    items_to_fetch.append(item_data)
            elif selected_attribute == "artist_name":
                if not matching_rows.empty and name:
                    row = matching_rows.iloc[0]
                    item_data = {
                        "name": name,
                        "type": "artist",
                        "cache_key": cache_key,
                        "track_uri": row["track_uri"],
                        "artist_name": name,
                    }
                    items_to_fetch.append(item_data)

    if items_to_fetch:
        try:
            batch_results = fetch_images_batch(items_to_fetch, target_size)
        except Exception as e:
            print(f"[ERROR] Batch image API failed: {e}")
            # Mark all items as missing in cache to avoid repeated attempts
            for item in items_to_fetch:
                image_cache[item["cache_key"]] = None
            return  # Fail fast, do not attempt downloads

        # prepare download tasks
        download_tasks = []
        for item in items_to_fetch:
            image_url = None
            if item["type"] == "track":
                image_url = batch_results.get(
                    item.get("track_uri")
                ) or batch_results.get(item["name"])
            elif item["type"] == "album":
                image_url = batch_results.get(
                    item.get("track_uri")
                ) or batch_results.get(item["name"])
            elif item["type"] == "artist":
                image_url = batch_results.get(item["name"])

            if image_url:
                download_tasks.append(
                    {
                        "name": item["name"],
                        "cache_key": item["cache_key"],
                        "image_url": image_url,
                        "target_size": target_size,
                    }
                )
            else:
                print(f"No image URL found for {item['name']} (type: {item['type']})")

        # download images in parallel for efficiency
        if download_tasks:
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(_download_and_cache_image, task)
                    for task in download_tasks
                ]
                successful_downloads = 0
                for future in futures:
                    try:
                        if future.result():
                            successful_downloads += 1
                    except Exception as e:
                        print(f"[ERROR] Image download failed: {e}")

    for name, cache_key in zip(names, cache_keys):
        if cache_key in image_cache:
            pass


def _download_and_cache_image(task) -> bool:
    start_time = time.time()
    """Download and cache a single image. Designed for parallel execution."""
    name = task["name"]
    cache_key = task["cache_key"]
    image_url = task["image_url"]
    target_size = task["target_size"]
    try:
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        img = Image.open(BytesIO(response.content))
        img_resized = img.resize((target_size, target_size), RESAMPLING_FILTER)
        color = get_dominant_color(img_resized, name)
        image_cache[cache_key] = {"img": img_resized, "color": color}
        elapsed = time.time() - start_time
        # print(
        #     f"[DEBUG] _download_and_cache_image: success for {name} ({elapsed:.2f} seconds)"
        # )
        return True
    except Exception:
        image_cache[cache_key] = None
        elapsed = time.time() - start_time
        print(
            f"[DEBUG] _download_and_cache_image: fail for {name} ({elapsed:.2f} seconds)"
        )
        return False


def precompute_data(
    monthly_df, selected_attribute, analysis_metric, top_n, start_date, end_date
) -> tuple:
    start_time = time.time()
    """Precompute cumulative data and rankings for all timestamps.

    Returns:
        tuple[list[pandas.Timestamp], dict]: (timestamps, precomputed_data)
        where precomputed_data maps timestamp -> dict(widths, labels, names, artist_names)
    """

    monthly_df["Date"] = monthly_df["Date"].dt.to_timestamp()
    # skip first 3 days for cleaner inital frame
    start_date = start_date + pd.Timedelta(days=3)
    timestamps = sorted(monthly_df["Date"].unique())
    timestamps = [
        ts for ts in timestamps if start_date <= ts <= monthly_df["Date"].max()
    ]
    timestamps = timestamps[::days]
    if timestamps[-1] != end_date:
        if end_date > timestamps[-1]:
            timestamps.append(end_date)
        else:
            timestamps[-1] = end_date
        timestamps.sort()

    precomputed_data = {}
    for ts in timestamps:
        cumulative_df = monthly_df[monthly_df["Date"] <= ts]

        if selected_attribute == "track_name":
            current_df = cumulative_df.groupby(
                ["track_uri"], as_index=False, observed=True
            ).agg(
                {
                    f"Cumulative_{analysis_metric}": "max",
                    "track_uri": "first",
                    "artist_name": "first",
                    "track_name": "first",
                }
            )
        elif selected_attribute == "album_name":
            current_df = cumulative_df.groupby(
                [selected_attribute, "artist_name"], as_index=False, observed=True
            ).agg(
                {
                    f"Cumulative_{analysis_metric}": "max",
                    "track_uri": "first",
                    "artist_name": "first",
                    "album_name": "first",
                }
            )
            current_df["album_artist"] = (
                current_df["album_name"] + " - " + current_df["artist_name"]
            )
            current_df = current_df.drop_duplicates(subset=["album_artist"])
        else:  # artist_name
            current_df = cumulative_df.groupby(
                [selected_attribute], as_index=False, observed=True
            ).agg(
                {
                    f"Cumulative_{analysis_metric}": "max",
                    "track_uri": "first",
                }
            )
            current_df["artist_name"] = current_df[selected_attribute]

        if selected_attribute == "album_name":
            prev_names = current_df["album_artist"].tolist()
            current_df["prev_rank"] = (
                current_df["album_artist"]
                .map({name: i for i, name in enumerate(prev_names)})
                .fillna(top_n)
            )
        elif selected_attribute == "track_name":
            prev_names = current_df["track_uri"].tolist()
            current_df["prev_rank"] = (
                current_df["track_uri"]
                .map({name: i for i, name in enumerate(prev_names)})
                .fillna(top_n)
            )
        else:  # artist_name
            prev_names = current_df["artist_name"].tolist()
            current_df["prev_rank"] = (
                current_df["artist_name"]
                .map({name: i for i, name in enumerate(prev_names)})
                .fillna(top_n)
            )
        top_n_df = (
            current_df.sort_values(
                [f"Cumulative_{analysis_metric}", "prev_rank"],
                ascending=[False, True],
            )
            .head(top_n)
            .reset_index(drop=True)
        )
        widths = [
            row[f"Cumulative_{analysis_metric}"] for _, row in top_n_df.iterrows()
        ] + [0] * (top_n - len(top_n_df))
        labels = []
        for _, row in top_n_df.iterrows():
            if selected_attribute == "track_name":
                song_name = "\n".join(textwrap.wrap(str(row["track_name"]), width=22))
                labels.append(song_name)
            elif selected_attribute == "album_name":
                album_name = "\n".join(textwrap.wrap(str(row["album_name"]), width=22))
                labels.append(album_name)
            else:
                labels.append(
                    "\n".join(textwrap.wrap(str(row[selected_attribute]), width=20))
                )
        labels += [""] * (top_n - len(top_n_df))
        if selected_attribute == "track_name":
            names = top_n_df["track_uri"].tolist() + [""] * (top_n - len(top_n_df))
            artist_names = top_n_df["artist_name"].tolist() + [""] * (
                top_n - len(top_n_df)
            )
        elif selected_attribute == "album_name":
            names = top_n_df["album_artist"].tolist() + [""] * (top_n - len(top_n_df))
            artist_names = top_n_df["artist_name"].tolist() + [""] * (
                top_n - len(top_n_df)
            )
        else:  # artist_name
            names = top_n_df["artist_name"].tolist() + [""] * (top_n - len(top_n_df))
            artist_names = [""] * top_n  # No sublabel for artist mode
        precomputed_data[ts] = {
            "widths": widths,
            "labels": labels,
            "names": names,
            "artist_names": artist_names,
        }
    elapsed = time.time() - start_time
    print(f"[DEBUG] precompute_data: end ({elapsed:.2f} seconds)")
    return timestamps, precomputed_data


def create_bar_animation(
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
) -> animation.FuncAnimation:
    start_time = time.time()
    print("[DEBUG] create_bar_animation: start")
    """Prepare the bar chart animation with optimized runtime.

    Returns:
        matplotlib.animation.FuncAnimation: Configured animation object.
    """
    t0 = time.time()
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    fig.patch.set_facecolor("#F0F0F0")
    plt.subplots_adjust(left=0.27, right=0.85, top=0.8, bottom=0.13)
    t1 = time.time()
    print(f"Time for figure setup: {t1 - t0:.2f} seconds")
    font_prop_heading, font_path_labels = get_fonts()
    title_map = {
        ("artist_name", "Streams"): "Most Played Artists",
        ("track_name", "Streams"): "Most Played Songs",
        ("album_name", "Streams"): "Most Played Albums",
        ("artist_name", "duration_ms"): "Most Played Artists",
        ("track_name", "duration_ms"): "Most Played Songs",
        ("album_name", "duration_ms"): "Most Played Albums",
    }

    title = title_map.get((selected_attribute, analysis_metric), "Most Played Albums")
    fig.suptitle(
        title.format(top_n=top_n),
        y=0.93,
        x=0.54,
        fontsize=56,
        fontproperties=font_prop_heading,
    )
    fig.text(
        0.60,
        0.060,
        "www.viztracks.com",
        ha="right",
        va="bottom",
        fontproperties=font_prop_heading,
        fontsize=24,
        color="#bed1bc",
        transform=fig.transFigure,
    )

    # Load Spotify Image
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    img_path = os.path.join(
        repo_root,
        "assets",
        "2024 Spotify Brand Assets",
        "Spotify_Full_Logo_RGB_Green.png",
    )
    img = mpimg.imread(img_path)
    image_axes = fig.add_axes([0.38, 0.555, 0.29, 0.59])
    image_axes.imshow(img)
    image_axes.axis("off")
    item_type = {"artist_name": "artist", "track_name": "track", "album_name": "album"}[
        selected_attribute
    ]
    df["Date"] = pd.to_datetime(df["Date"])
    df["Date"] = df["Date"].dt.to_period(period)
    monthly_df = df
    t2 = time.time()
    print(f"Time for data preprocessing: {t2 - t1:.2f} seconds")
    t3 = time.time()
    # Precompute data to avoid per-frame aggregation for efficiency
    timestamps, precomputed_data = precompute_data(
        monthly_df,
        selected_attribute,
        analysis_metric,
        top_n,
        start_date,
        end_date,
    )
    t4 = time.time()
    print(f"Time for precomputing data: {t4 - t3:.2f} seconds")

    # Image scaling and positioning
    top_n_scale_mapping_height = {
        1: 70,
        2: 70,
        3: 75,
        4: 80,
        5: 80,
        6: 80,
        7: 80,
        8: 80,
        9: 80,
        10: 82,
    }
    scale_factor = top_n_scale_mapping_height.get(top_n)
    bar_height = {
        1: 3.0,
        2: 3.0,
        3: 2.5,
        4: 1.7,
        5: 1.4,
        6: 1.1,
        7: 0.9,
        8: 0.8,
        9: 0.75,
        10: 0.7,
    }.get(top_n)
    target_size = int(bar_height * scale_factor)
    t5 = time.time()
    # Batch preload images
    if selected_attribute == "track_name":
        all_names = monthly_df["track_uri"].unique()
    elif selected_attribute == "album_name":
        all_names = (
            monthly_df["album_name"] + " - " + monthly_df["artist_name"]
        ).unique()
    else:
        all_names = monthly_df[selected_attribute].unique()

    # Get all unique names to be used in the animation
    used_names = set()
    for frame_data in precomputed_data.values():
        used_names.update(frame_data["names"])
    used_names = {name for name in used_names if name}

    # Only preload images for names that are needed
    all_names = [name for name in all_names if name in used_names]
    print(f"Preloading images for {len(all_names)} unique items...")
    preload_images_batch(
        all_names, monthly_df, selected_attribute, item_type, top_n, target_size
    )
    t6 = time.time()
    print(f"Time for preloading images: {t6 - t5:.2f} seconds")
    # Start all bars off-screen
    if top_n == 1:
        initial_positions = [-1]
    else:
        initial_positions = [-1] * top_n

    # Precomputed target positions were unused; removed to reduce lint noise.
    initial_labels = [""] * top_n
    bars = ax.barh(
        initial_positions,
        [0] * top_n,
        alpha=0.7,
        height=bar_height,
        edgecolor="#D3D3D3",
        linewidth=1.2,
    )
    ax.set_yticks([])
    ax.tick_params(axis="y", which="both", length=0, pad=15)
    ax.xaxis.label.set_fontproperties(font_path_labels)
    ax.xaxis.label.set_size(18)
    ax.xaxis.set_label_coords(-0.95, -0.05)
    setup_bar_plot_style(ax)

    # Compute a fixed global x-limit for blitting (no autoscale per frame)
    try:
        global_max = max(max(d["widths"]) for d in precomputed_data.values()) or 1
    except Exception:
        global_max = 1
    ax.set_xlim(0, global_max * 1.15)

    top_gap = 0.3
    bottom_gap = 0.2

    if top_n == 1:
        ax.set_ylim(4.5 - bottom_gap - bar_height / 2, 4.5 + top_gap + bar_height / 2)
    else:
        positions = [8.9 - i * (8.6 / (top_n - 1)) for i in range(top_n)]
        top_pos = max(positions)
        bottom_pos = min(positions)
        ax.set_ylim(
            bottom_pos - bottom_gap - bar_height / 2, top_pos + top_gap + bar_height / 2
        )
    # how far to the left of the bar to place the image
    top_n_xybox_mapping = {
        1: (-127, 0),
        2: (-127, 0),
        3: (-113, 0),
        4: (-80, 0),
        5: (-69, 0),
        6: (-57, 0),
        7: (-47, 0),
        8: (-41, 0),
        9: (-39, 0),
        10: (-36, 0),
    }
    # Pre-allocate text and image annotations
    text_objects = []
    label_objects = []
    artist_label_objects = []
    image_annotations = []
    offset_images = []
    blank_img = np.zeros((target_size, target_size, 3), dtype=np.uint8)
    xybox = top_n_xybox_mapping.get(top_n)
    for i in range(top_n):
        # bar numbers text
        text_obj = ax.text(
            0,
            i,
            "",
            va="center",
            ha="left",
            fontsize=24,
            fontproperties=font_path_labels,
            visible=False,
        )
        text_objects.append(text_obj)

        # y-axis labels
        label_obj = ax.text(
            0,
            i,
            "",
            va="center",
            ha="right",
            fontsize=22,
            fontproperties=font_path_labels,
            visible=False,
        )
        label_objects.append(label_obj)

        # y-axis labels subtext
        artist_obj = ax.text(
            0,
            i,
            "",
            va="center",
            ha="right",
            fontsize=20,
            fontproperties=font_path_labels,
            color="#A9A9A9",
            visible=False,
        )
        artist_label_objects.append(artist_obj)

        # Pre-create OffsetImage and AnnotationBbox for each bar
        offset_img = OffsetImage(blank_img, zoom=1)
        ab = AnnotationBbox(
            offset_img,
            (0, 0),
            xybox=xybox,
            xycoords="data",
            boxcoords="offset points",
            frameon=False,
            visible=False,
            bboxprops=dict(
                boxstyle="round,pad=0.05",
                edgecolor="#A9A9A9",
                facecolor="#DCDCDC",
                linewidth=0.5,
            ),
        )
        ax.add_artist(ab)
        image_annotations.append(ab)
        offset_images.append(offset_img)

    # Add year and month text boxes
    year_text = ax.text(
        0.78,
        0.10,
        "",
        transform=ax.transAxes,
        fontsize=38,
        fontproperties=font_prop_heading,
        bbox=dict(facecolor="#F0F0F0", edgecolor="none", alpha=0.7),
        color="#A9A9A9",
    )
    month_text = ax.text(
        0.78,
        0.05,
        "",
        transform=ax.transAxes,
        fontsize=38,
        fontproperties=font_prop_heading,
        bbox=dict(facecolor="#F0F0F0", edgecolor="none", alpha=0.7),
        color="#A9A9A9",
    )
    # x-axis label for clarity
    ax.text(
        0.38,
        -0.033,
        "Streams" if analysis_metric == "Streams" else "Minutes Listened",
        transform=ax.transAxes,
        fontsize=28,
        fontproperties=font_prop_heading,
        bbox=dict(facecolor="#F0F0F0", edgecolor="none", alpha=0.7),
        color="#A9A9A9",
        ha="center",
        va="top",
    )

    interp_steps = interp_steps

    initial_top_sorted = (
        monthly_df[monthly_df["Date"] <= timestamps[0]]
        .nlargest(top_n, f"Cumulative_{analysis_metric}")
        .sort_values(f"Cumulative_{analysis_metric}", ascending=False)
    )
    # Initial widths are directly derived in the first frame; removing unused variable.

    if selected_attribute == "track_name":
        initial_labels = [
            "\n".join(textwrap.wrap(row["track_name"], width=22))
            for _, row in initial_top_sorted.iterrows()
        ] + [""] * (top_n - len(initial_top_sorted))
        initial_names = initial_top_sorted["track_uri"].tolist() + [""] * (
            top_n - len(initial_top_sorted)
        )
    elif selected_attribute == "album_name":
        initial_labels = [
            "\n".join(textwrap.wrap(row["album_name"], width=22))
            for _, row in initial_top_sorted.iterrows()
        ] + [""] * (top_n - len(initial_top_sorted))
        initial_names = initial_top_sorted["album_name"].tolist() + [""] * (
            top_n - len(initial_top_sorted)
        )
    else:  # artist_name
        initial_labels = [
            "\n".join(textwrap.wrap(row["artist_name"], width=20))
            for _, row in initial_top_sorted.iterrows()
        ] + [""] * (top_n - len(initial_top_sorted))
        initial_names = initial_top_sorted["artist_name"].tolist() + [""] * (
            top_n - len(initial_top_sorted)
        )

    anim_state = AnimationState(top_n)
    anim_state.prev_labels = initial_labels[:]
    anim_state.prev_widths = [0] * top_n
    anim_state.prev_names = initial_names[:]
    anim_state.prev_positions = [-1] * top_n  # Start off-screen
    anim_state.prev_interp_positions = [-1] * top_n  # Start off-screen
    anim_state.last_img_obj = [None] * top_n

    for i, name in enumerate(initial_names):
        if name:
            cache_key = f"{name}_top_n_{top_n}"
            img_data = image_cache.get(cache_key)
            if img_data and img_data["color"]:
                bars[i].set_facecolor(np.array(img_data["color"]) / 255)

    total_frames = len(timestamps) * interp_steps
    print(f"Total frames: {total_frames}")

    def quadratic_ease_in_out(t) -> float:
        """Quadratic ease-in-out function to handle smooth transitions."""
        return t * t * (3 - 2 * t)

    # Pre-build a flat list of all artists for blitting convenience
    all_artists = (
        list(bars)
        + text_objects
        + label_objects
        + artist_label_objects
        + image_annotations
        + [year_text, month_text]
    )

    def _init_blit():
        # Hide everything initially for blitting baseline
        for bar in bars:
            bar.set_visible(False)
        for t in text_objects:
            t.set_visible(False)
        for t in label_objects:
            t.set_visible(False)
        for t in artist_label_objects:
            t.set_visible(False)
        for ab in image_annotations:
            ab.set_visible(False)
        year_text.set_text("")
        month_text.set_text("")
        return all_artists

    def animate(frame):
        """Update the bar chart for each frame."""
        nonlocal anim_state
        main_frame = frame // interp_steps
        sub_step = frame % interp_steps
        current_time = timestamps[main_frame]

        # Use precomputed data
        data = precomputed_data[current_time]
        widths = data["widths"]
        labels = data["labels"]
        names = data["names"]
        artist_names = data["artist_names"]

        if top_n == 1:
            target_positions = [4.5]
        else:
            target_positions = [8.9 - i * (8.6 / (top_n - 1)) for i in range(top_n)]

        if sub_step == 0:
            if frame == 0:
                new_positions = target_positions[:]
                start_positions = [-1] * top_n
                anim_state.current_new_positions = new_positions[:]
            else:
                new_positions = target_positions[:]
                anim_state.current_new_positions = new_positions[:]
                bar_mapping = [None] * top_n
                for i, name in enumerate(names):
                    if name in anim_state.prev_names:
                        prev_idx = anim_state.prev_names.index(name)
                        bar_mapping[i] = prev_idx
                    else:
                        bar_mapping[i] = None
                start_positions = []
                for i, name in enumerate(names):
                    if bar_mapping[i] is not None:
                        start_positions.append(
                            anim_state.prev_interp_positions[bar_mapping[i]]
                        )
                    else:
                        start_positions.append(-1)  # Enter from off-screen
        else:
            new_positions = anim_state.current_new_positions[:]
            start_positions = anim_state.prev_interp_positions[:]

        t = sub_step / (interp_steps - 1) if interp_steps > 1 else 1.0
        t_eased = quadratic_ease_in_out(t)

        interp_positions = [
            min(
                max(
                    start_positions[i]
                    + (new_positions[i] - start_positions[i]) * t_eased,
                    -1,
                ),
                9,
            )
            for i in range(top_n)
        ]
        interp_widths = [
            (
                anim_state.prev_widths[i]
                + (widths[i] - anim_state.prev_widths[i]) * t_eased
                if i < len(anim_state.prev_widths)
                else widths[i] * t_eased
            )
            for i in range(top_n)
        ]

        max_width = max(interp_widths) if interp_widths else 1

        # this section ensures that the minimum bar width is applied
        # and the image does not go below 0 on the x-axis
        min_bar_width_mapping = {
            1: 0.30,
            2: 0.54,
            3: 0.37,
            4: 0.28,
            5: 0.22,
            6: 0.19,
            7: 0.16,
            8: 0.14,
            9: 0.13,
            10: 0.11,
        }

        min_bar_width_multiplier = min_bar_width_mapping.get(top_n, 0.11)
        min_bar_width = max_width * min_bar_width_multiplier
        display_widths = []
        active_bars = []

        for i, width in enumerate(interp_widths):
            name = names[i] if i < len(names) else ""
            has_data = width > 0 and name

            if has_data:
                display_widths.append(max(width, min_bar_width))
                active_bars.append(True)
            else:
                display_widths.append(0)
                active_bars.append(False)

        # handle first frame specially
        if frame == 0 and sub_step == 0:
            display_widths = [0] * top_n
            interp_positions = [-1] * top_n
            active_bars = [False] * top_n

        changed = []
        for i, bar in enumerate(bars):
            if active_bars[i]:
                bar.set_width(display_widths[i])
                bar.set_y(interp_positions[i] - bar_height / 2)
                bar.set_visible(True)
                changed.append(bar)
            else:
                bar.set_width(0)
                bar.set_y(-1)  # Move off-screen
                bar.set_visible(False)  # Hide completely
                changed.append(bar)

        max_value = max(display_widths) if display_widths else 1
        offset = max(0.01, max_value * 0.03)

        # dynamic label font size based on top_n
        if selected_attribute in ["track_name", "album_name"]:
            top_n_label_fontsize_mapping = {
                1: 22,
                2: 22,
                3: 22,
                4: 22,
                5: 22,
                6: 20,
                7: 20,
                8: 20,
                9: 19,
                10: 19,
            }
            label_fontsize = top_n_label_fontsize_mapping.get(top_n, 22)
        else:
            label_fontsize = 22

        for i in range(top_n):
            name = names[i] if i < len(names) else ""
            text_x = display_widths[i]
            bar_center_y = interp_positions[i]
            has_data = active_bars[i] if active_bars else (text_x > 0 and name)

            if frame == 0 and sub_step == 0:
                # Hide all objects on first frame
                text_objects[i].set_visible(False)
                label_objects[i].set_visible(False)
                artist_label_objects[i].set_visible(False)
                image_annotations[i].set_visible(False)
                anim_state.last_img_obj[i] = None  # Reset last image
                changed.extend(
                    [
                        text_objects[i],
                        label_objects[i],
                        artist_label_objects[i],
                        image_annotations[i],
                    ]
                )
            elif has_data:  # Only show elements for bars with data
                text_objects[i].set_position((text_x + offset, bar_center_y))
                text_objects[i].set_text(f"{interp_widths[i]:,.0f}")
                text_objects[i].set_fontsize(24)
                text_objects[i].set_visible(True)
                changed.append(text_objects[i])

                # Update main label text with proper formatting
                if i < len(labels) and labels[i]:
                    label_objects[i].set_position((-offset, bar_center_y))
                    label_objects[i].set_text(labels[i])
                    label_objects[i].set_fontsize(label_fontsize)
                    label_objects[i].set_visible(True)
                    changed.append(label_objects[i])
                else:
                    label_objects[i].set_visible(False)
                    changed.append(label_objects[i])

                if selected_attribute in ["track_name", "album_name"]:
                    if i < len(artist_names) and artist_names[i]:
                        artist_name = f"({artist_names[i]})"
                        artist_wrapped = "\n".join(textwrap.wrap(artist_name, width=30))

                        # calculate vertical offset for subtext labels
                        song_lines = labels[i].count("\n") + 1 if i < len(labels) else 1
                        line_spacing_mapping = {
                            1: {1: 0.44, 2: 0.55, 3: 0.72},
                            2: {1: 0.44, 2: 0.55, 3: 0.72},
                            3: {1: 0.38, 2: 0.50, 3: 0.66},
                            4: {1: 0.34, 2: 0.45, 3: 0.60},
                            5: {1: 0.30, 2: 0.40, 3: 0.53},
                            6: {1: 0.27, 2: 0.36, 3: 0.48},
                            7: {1: 0.25, 2: 0.35, 3: 0.46},
                            8: {1: 0.24, 2: 0.34, 3: 0.44},
                            9: {1: 0.23, 2: 0.33, 3: 0.43},
                            10: {1: 0.22, 2: 0.32, 3: 0.42},
                        }
                        top_n_spacing = line_spacing_mapping.get(top_n, {})
                        artist_y_offset = top_n_spacing.get(song_lines, 0.30)

                        artist_label_objects[i].set_position(
                            (-offset, bar_center_y - artist_y_offset)
                        )
                        artist_label_objects[i].set_text(artist_wrapped)
                        artist_label_objects[i].set_fontsize(label_fontsize - 2)
                        artist_label_objects[i].set_visible(True)
                        changed.append(artist_label_objects[i])
                    else:
                        artist_label_objects[i].set_visible(False)
                        changed.append(artist_label_objects[i])
                else:
                    artist_label_objects[i].set_visible(False)
                    changed.append(artist_label_objects[i])

                cache_key = f"{name}_top_n_{top_n}"
                if selected_attribute == "track_name" and i < len(names):
                    current_frame_data = monthly_df[monthly_df["Date"] <= current_time]
                    matching_rows = current_frame_data[
                        current_frame_data[selected_attribute] == name
                    ]
                    if not matching_rows.empty:
                        row = matching_rows.iloc[-1]
                        if "track_uri" in row and row["track_uri"]:
                            cache_key = f"{row['track_uri']}_top_n_{top_n}"

                if selected_attribute == "album_name":
                    cache_key = f"{name}_album_top_n_{top_n}"

                elif selected_attribute == "artist_name":
                    cache_key = f"{name}_top_n_{top_n}"

                img_data = image_cache.get(cache_key)

                if img_data and text_x > 0 and name:
                    # Only update OffsetImage if the image object has changed
                    img_obj = img_data["img"]
                    if anim_state.last_img_obj[i] is not img_obj:
                        offset_images[i].set_data(np.array(img_obj))
                        anim_state.last_img_obj[i] = img_obj
                    image_annotations[i].xy = (text_x, bar_center_y)
                    image_annotations[i].set_visible(True)
                    changed.append(image_annotations[i])
                    if img_data["color"]:
                        bars[i].set_facecolor(np.array(img_data["color"]) / 255)
                else:
                    image_annotations[i].set_visible(False)
                    anim_state.last_img_obj[i] = None
                    changed.append(image_annotations[i])
            else:
                text_objects[i].set_visible(False)
                label_objects[i].set_visible(False)
                artist_label_objects[i].set_visible(False)
                image_annotations[i].set_visible(False)
                anim_state.last_img_obj[i] = None
                changed.extend(
                    [
                        text_objects[i],
                        label_objects[i],
                        artist_label_objects[i],
                        image_annotations[i],
                    ]
                )

        # update the state for the next frame
        if sub_step == interp_steps - 1:
            anim_state.prev_widths[:] = widths
            anim_state.prev_names[:] = names
            anim_state.prev_positions[:] = new_positions[:]
            anim_state.prev_interp_positions = target_positions[:]
        else:
            anim_state.prev_interp_positions = interp_positions[:]

        # Axes are static for blitting; avoid per-frame ticks/limits
        # update year and month text
        year_text.set_text(f"{current_time.year}")
        month_text.set_text(f"{current_time.strftime('%B')}")
        changed.extend([year_text, month_text])
        return changed

    # t_text = time.time()
    # print(f"[TIMING] frame={frame} total={t_text - t_start:.4f}s")

    t7 = time.time()
    print(f"Time for animation setup: {t7 - t6:.2f} seconds")
    anim = animation.FuncAnimation(
        fig,
        animate,
        frames=total_frames,
        init_func=_init_blit,
        blit=True,
        interval=1,
        repeat=False,
    )
    t8 = time.time()
    print(f"Total animation creation time: {t8 - t7:.2f} seconds")
    elapsed = time.time() - start_time
    print(f"create_bar_animation: end ({elapsed:.2f} seconds)")
    try:
        setattr(anim, "total_frames", int(total_frames))
    except Exception:
        pass
    return anim
