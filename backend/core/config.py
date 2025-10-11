"""Centralized configuration for the backend.

Loads environment variables, sets defaults, and exposes constants
used across services and routes.
"""

import os

from dotenv import load_dotenv

load_dotenv("env/.env")

ENV = os.getenv("ENV", "local")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# Session storage
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/tmp/spotify_sessions")
MAX_SESSIONS = int(os.getenv("MAX_SESSIONS", "5"))
