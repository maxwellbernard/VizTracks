import json
import os
import uuid
import zipfile

import duckdb
import pandas as pd
import polars as pl

from backend.core.config import UPLOAD_DIR


def insert_jsons_from_zip_to_duckdb(
    zip_path: str, session_id: str | None = None
) -> tuple[str, pd.Timestamp | None, pd.Timestamp | None]:
    """Ingest Spotify JSONs from a zip into a per-session DuckDB database.

    Filters out plays under 30 seconds. Creates or appends to a table named
    ``spotify_data`` with typed columns.

    Args:
        zip_path: Path to the uploaded zip file containing JSONs.
        session_id: Optional session id. If None, generates a UUID.

    Returns:
        The session_id used, and the min/max Date found in the ingested data
        (None if no rows).
    """
    if session_id is None:
        session_id = str(uuid.uuid4())
    db_path = os.path.join(UPLOAD_DIR, f"spotify_session_{session_id}.duckdb")
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS spotify_data (
            Date TIMESTAMP,
            duration_ms BIGINT,
            track_name VARCHAR,
            artist_name VARCHAR,
            album_name VARCHAR,
            track_uri VARCHAR
        )
    """)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        file_list = zip_ref.namelist()
        json_file_names = [
            f
            for f in file_list
            if f.endswith(".json") and "Audio" in f and not f.endswith("/")
        ]
        for json_file_name in json_file_names:
            try:
                with zip_ref.open(json_file_name) as json_file:
                    json_content = json_file.read()
                    json_data = json.loads(json_content.decode("utf-8"))
                    if json_data:
                        filtered_data = [
                            {
                                "Date": row.get("ts"),
                                "duration_ms": row.get("ms_played") / 60000
                                if row.get("ms_played")
                                else None,
                                "track_name": row.get("master_metadata_track_name"),
                                "artist_name": row.get(
                                    "master_metadata_album_artist_name"
                                ),
                                "album_name": row.get(
                                    "master_metadata_album_album_name"
                                ),
                                "track_uri": row.get("spotify_track_uri"),
                            }
                            for row in json_data
                            if row.get("ms_played", 0) > 30000
                        ]
                        if filtered_data:
                            df = pl.DataFrame(filtered_data)
                            df = df.with_columns(
                                pl.col("Date").str.strptime(
                                    pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False
                                )
                            )
                            df = df.drop_nulls()
                            con.execute("INSERT INTO spotify_data SELECT * FROM df")
            except Exception as e:
                print(f"Warning: Could not process {json_file_name}: {e}")
                continue

    min_date = con.execute("SELECT MIN(Date) FROM spotify_data").fetchone()[0]
    max_date = con.execute("SELECT MAX(Date) FROM spotify_data").fetchone()[0]
    con.close()
    return session_id, min_date, max_date


def query_user_duckdb(
    session_id: str,
    selected_attribute: str,
    analysis_metric: str,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    top_n: int,
) -> pd.DataFrame | None:
    """Return a top-N aggregate for the specified attribute and metric.

    Args:
        session_id: Session identifier for DuckDB file.
        selected_attribute: One of artist_name, track_name, album_name.
        analysis_metric: "Streams" or "duration_ms".
        start_date: Inclusive start date.
        end_date: Inclusive end date.
        top_n: Number of rows to return.

    Returns:
        Aggregated result, or None if DB is missing.
    """
    db_path = os.path.join(UPLOAD_DIR, f"spotify_session_{session_id}.duckdb")
    if not os.path.exists(db_path):
        return None
    metric_expr = (
        "COUNT(*) as Streams"
        if analysis_metric == "Streams"
        else "SUM(duration_ms) as duration_ms"
    )
    order_by = "Streams" if analysis_metric == "Streams" else "duration_ms"

    if selected_attribute == "artist_name":
        select_cols = "artist_name, MIN(track_uri) as track_uri"
        group_by = "artist_name"
    elif selected_attribute == "track_name":
        select_cols = "track_name, artist_name, track_uri"
        group_by = "track_name, artist_name, track_uri"
    elif selected_attribute == "album_name":
        select_cols = "album_name, artist_name, MIN(track_uri) as track_uri"
        group_by = "album_name, artist_name"
    else:
        select_cols = f"{selected_attribute}, MIN(track_uri) as track_uri"
        group_by = selected_attribute

    end_date_inclusive = (pd.to_datetime(end_date) + pd.Timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    query = f"""
        SELECT {select_cols}, {metric_expr}
        FROM spotify_data
        WHERE Date >= '{start_date}' AND Date < '{end_date_inclusive}'
        GROUP BY {group_by}
        ORDER BY {order_by} DESC
        LIMIT {top_n}
    """
    con = duckdb.connect(db_path)
    result_df = con.execute(query).df()
    con.close()
    return result_df


def query_user_duckdb_for_animation(
    session_id: str,
    selected_attribute: str,
    analysis_metric: str,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    filter_number: int = 100,
) -> pd.DataFrame | None:
    """Return a time-indexed dataset suitable for animation frames.

    Selects top entities within the window, then returns daily rows per entity
    with both per-day metric and a cumulative metric for smoother animations.

    Args:
        session_id: Session identifier for DuckDB file.
        selected_attribute: artist_name, track_name, or album_name.
        analysis_metric: "Streams" or "duration_ms".
        start_date: Inclusive start date.
        end_date: Inclusive end date.
        filter_number: Count of top entities to keep in the window.

    Returns:
        Long-form result by entity and date, or None if DB is missing.
    """
    db_path = os.path.join(UPLOAD_DIR, f"spotify_session_{session_id}.duckdb")
    if not os.path.exists(db_path):
        return None

    if analysis_metric == "Streams":
        metric_expr = "COUNT(*) as Streams"
        cumsum_expr = "SUM(COUNT(*)) OVER (PARTITION BY {group_by} ORDER BY Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as Cumulative_Streams"
        order_by = "Streams"
    elif analysis_metric == "duration_ms":
        metric_expr = "SUM(duration_ms) as duration_ms"
        cumsum_expr = "SUM(SUM(duration_ms)) OVER (PARTITION BY {group_by} ORDER BY Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as Cumulative_duration_ms"
        order_by = "duration_ms"

    end_date_inclusive = (pd.to_datetime(end_date) + pd.Timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    con = duckdb.connect(db_path)

    if selected_attribute == "artist_name":
        top_entities_query = f"""
            SELECT artist_name, COUNT(*) as Streams, SUM(duration_ms) as duration_ms
            FROM spotify_data
            WHERE Date >= '{start_date}' AND Date < '{end_date_inclusive}'
            GROUP BY artist_name
            ORDER BY {order_by} DESC
            LIMIT {filter_number}
        """
        top_entities = [row[0] for row in con.execute(top_entities_query).fetchall()]
        group_by = "artist_name"
        query = f"""
            SELECT
                artist_name,
                Date,
                regexp_extract(MIN(track_uri), '[^:]+$', 0) as track_uri,
                {metric_expr},
                {cumsum_expr.format(group_by=group_by)}
            FROM spotify_data
            WHERE Date >= '{start_date}' AND Date < '{end_date_inclusive}'
            AND artist_name IN ({",".join(["?"] * len(top_entities))})
            GROUP BY artist_name, Date
            ORDER BY artist_name, Date
        """
        result_df = con.execute(query, top_entities).df()

    elif selected_attribute == "track_name":
        top_entities_query = f"""
            SELECT track_uri, COUNT(*) as Streams, SUM(duration_ms) as duration_ms
            FROM spotify_data
            WHERE Date >= '{start_date}' AND Date < '{end_date_inclusive}'
            GROUP BY track_uri
            ORDER BY {order_by} DESC
            LIMIT {filter_number}
        """
        top_entities = [row[0] for row in con.execute(top_entities_query).fetchall()]
        group_by = "track_uri"
        query = f"""
            SELECT
                track_name,
                artist_name,
                Date,
                MIN(track_uri) as track_uri,
                {metric_expr},
                {cumsum_expr.format(group_by=group_by)}
            FROM spotify_data
            WHERE Date >= '{start_date}' AND Date < '{end_date_inclusive}'
            AND track_uri IN ({",".join(["?"] * len(top_entities))})
            GROUP BY track_name, track_uri, artist_name, Date
            ORDER BY track_name, Date
        """
        result_df = con.execute(query, top_entities).df()

    elif selected_attribute == "album_name":
        top_entities_query = f"""
            SELECT album_name, artist_name, COUNT(*) as Streams, SUM(duration_ms) as duration_ms
            FROM spotify_data
            WHERE Date >= '{start_date}' AND Date < '{end_date_inclusive}'
            GROUP BY album_name, artist_name
            ORDER BY {order_by} DESC
            LIMIT {filter_number}
        """
        top_entities = [
            (row[0], row[1]) for row in con.execute(top_entities_query).fetchall()
        ]
        group_by = "album_name"
        query = f"""
            SELECT
                album_name,
                artist_name,
                Date,
                MIN(track_uri) as track_uri,
                {metric_expr},
                {cumsum_expr.format(group_by=group_by)}
            FROM spotify_data
            WHERE Date >= '{start_date}' AND Date < '{end_date_inclusive}'
            GROUP BY album_name, track_uri, artist_name, Date
            ORDER BY album_name, Date
        """
        result_df = con.execute(query).df()

    con.close()
    if selected_attribute == "album_name":
        top_entities_set = set(top_entities)
        result_df = result_df[
            result_df.apply(
                lambda row: (row["album_name"], row["artist_name"]) in top_entities_set,
                axis=1,
            )
        ]
    return result_df
