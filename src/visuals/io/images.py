"""Image fetch utilities for Spotify items with batch endpoints."""

from __future__ import annotations

import os
import time
from typing import Dict, List

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


def _spotify_client() -> spotipy.Spotify:
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret
    )
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)


def fetch_images_batch(items_data: List[Dict], target_size: int) -> Dict[str, str]:
    """Fetch images in batches using Spotify's batch endpoints.

    Returns a mapping cache_key -> image_url.
    """
    sp = _spotify_client()
    image_urls: Dict[str, str] = {}

    tracks: list[dict] = []
    albums: list[dict] = []
    artists: list[dict] = []

    for item in items_data:
        if item["type"] == "track" and item.get("track_uri"):
            tracks.append(item)
        elif item["type"] == "album" and item.get("track_uri"):
            albums.append(item)
        elif item["type"] == "artist" and item.get("track_uri"):
            artists.append(item)

    if tracks:
        track_uris = [item["track_uri"] for item in tracks]
        track_images = _fetch_tracks_batch(sp, track_uris, target_size)
        image_urls.update(track_images)

    if albums:
        track_uris = [item["track_uri"] for item in albums]
        album_images = _fetch_tracks_batch(sp, track_uris, target_size)
        for item in albums:
            track_uri = item["track_uri"]
            if track_uri in album_images:
                album_name = item["name"]
                image_urls[album_name] = album_images[track_uri]

    if artists:
        artist_images = _fetch_artists_from_tracks_batch(sp, artists)
        image_urls.update(artist_images)

    return image_urls


def _fetch_tracks_batch(
    sp: spotipy.Spotify, track_uris: List[str], target_size: int
) -> Dict[str, str]:
    image_urls: Dict[str, str] = {}
    for i in range(0, len(track_uris), 50):
        batch = track_uris[i : i + 50]
        try:
            tracks_response = sp.tracks(batch)
            for track in tracks_response["tracks"]:
                if track and track["album"].get("images"):
                    images = track["album"]["images"]
                    images_sorted = sorted(images, key=lambda img: img["height"])
                    image_url = None
                    for img in images_sorted:
                        if img["height"] >= target_size:
                            image_url = img["url"]
                            break
                    if not image_url and images_sorted:
                        image_url = images_sorted[-1]["url"]
                    image_urls[track["uri"]] = image_url
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                print(f"Spotify Rate Limit: Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                return _fetch_tracks_batch(sp, track_uris[i:], target_size)
            print(f"Error fetching tracks batch: {e}")
        time.sleep(0.1)
    return image_urls


def _fetch_artists_from_tracks_batch(
    sp: spotipy.Spotify, artist_items: List[Dict]
) -> Dict[str, str]:
    image_urls: Dict[str, str] = {}

    track_ids = [item["track_uri"] for item in artist_items]
    uri_to_name = {item["track_uri"]: item["name"] for item in artist_items}

    artist_id_to_name: Dict[str, str] = {}
    all_artist_ids: list[str] = []

    for i in range(0, len(track_ids), 50):
        batch_track_ids = track_ids[i : i + 50]
        try:
            tracks_response = sp.tracks(batch_track_ids)
            for j, track in enumerate(tracks_response["tracks"]):
                if track and track.get("artists"):
                    track_id = batch_track_ids[j]
                    artist_name = uri_to_name.get(track_id)
                    for artist in track["artists"]:
                        if artist["name"] == artist_name:
                            artist_id = artist["id"]
                            artist_id_to_name[artist_id] = artist_name
                            all_artist_ids.append(artist_id)
                            break
            time.sleep(0.1)
        except Exception as e:
            print(f"Batch tracks API failed: {e}")
            continue

    if all_artist_ids:
        unique_artist_ids = list(dict.fromkeys(all_artist_ids))
        for i in range(0, len(unique_artist_ids), 50):
            batch_artist_ids = unique_artist_ids[i : i + 50]
            try:
                artists_response = sp.artists(batch_artist_ids)
                for artist in artists_response["artists"]:
                    if artist and artist.get("images"):
                        artist_id = artist["id"]
                        artist_name = artist_id_to_name.get(artist_id)
                        if artist_name:
                            image_url = artist["images"][0]["url"]
                            image_urls[artist_name] = image_url
                time.sleep(0.1)
            except Exception as e:
                print(f"Batch artists API failed: {e}")
                continue
    return image_urls
