"""Global caches and error tracking for visuals.

These are shared across modules to avoid redundant network calls and
computations during a single user session/process lifetime.
"""

image_cache: dict[str, str] = {}
color_cache: dict[str, tuple] = {}
error_logged: set[str] = set()
