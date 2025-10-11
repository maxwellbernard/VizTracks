# Backend (Flask API)

This folder contains the Flask backend that powers file uploads, data queries, and video/image generation.

## Structure

- app.py — Flask entrypoint; registers blueprints and starts the server on port 8080.
- core/
  - config.py — central configuration (env vars, paths); loads `env/.env` for local dev.
- routes/
  - uploads.py — `/process` endpoint for ZIP upload and DuckDB ingestion.
  - images.py — `/generate_image` endpoint (returns JPEG).
  - animations.py — `/generate_animation` endpoint (returns MP4).
- services/
  - db.py — DuckDB insert/query helpers.
  - encoding.py — centralized ffmpeg args and `encode_animation()`.
  - system.py — memory logging and session cleanup in `/tmp/spotify_sessions`.
  - visuals.py — thin wrappers calling `src.visuals` plot/animation functions.

## Environment variables

`backend/core/config.py` loads variables from `env/.env` and the environment.

- ENV (default: `local`)
- SUPABASE_URL, SUPABASE_KEY (analytics)
- UPLOAD_DIR (default: `/tmp/spotify_sessions`)
- MAX_SESSIONS (default: `5`)

Other parts of the app (image fetching in `src/visuals`) rely on:

- SPOTIFY_CLIENT_ID
- SPOTIFY_CLIENT_SECRET

## API overview

- POST `/process`
  - Form-data file field: `file` (the Spotify ZIP)
  - Response: `{ session_id, data_min_date, data_max_date }`

- POST `/generate_image`
  - JSON: `{ session_id, selected_attribute, analysis_metric, top_n, start_date, end_date }`
  - Response: `{ image: <base64>, filename }`

- POST `/generate_animation`
  - JSON: `{ session_id, selected_attribute, analysis_metric, top_n, start_date, end_date, speed_for_bar_animation, days, interp_steps, period, figsize, dpi }`
  - Response: `{ video: <base64>, filename }`

## Encoding settings

`services/encoding.py` sets mobile-compatible, fast ffmpeg defaults:
- libx264, `-preset ultrafast`, `-crf 30`
- `-pix_fmt yuv420p` (broad compatibility)
- `-movflags +faststart` (better streaming/thumbnailing)
- `-threads 0`, GOP ≈ 2s, and `-an`

## Notes

- This service uses DuckDB files per session under `/tmp/spotify_sessions`. `services/system.cleanup_old_sessions()` removes old files periodically.
- CORS is enabled in `app.py` to allow the Streamlit frontend to call the API.
