"""Input normalization for user-selected options.

Provides mappings from UI labels to internal column/metric names used by the
analytics pipeline.
"""

ATTRIBUTE_MAP = {
    "Artist": "artist_name",
    "Song": "track_name",
    "Album": "album_name",
}

METRIC_MAP = {
    "Number of Streams": "Streams",
    "Time Listened": "duration_ms",
}


def normalize_inputs(selected_attribute: str, analysis_metric: str) -> tuple[str, str]:
    """Normalize attribute and metric to internal identifiers.

    Args:
        selected_attribute: UI label like "Artist", "Song", or "Album".
        analysis_metric: UI label like "Number of Streams" or "Time Listened".

    Returns:
        A pair (normalized_attribute, normalized_metric),
        for example ("artist_name", "Streams").
    """
    norm_attr = ATTRIBUTE_MAP.get(selected_attribute, selected_attribute)
    norm_metric = METRIC_MAP.get(analysis_metric, analysis_metric)
    return norm_attr, norm_metric
