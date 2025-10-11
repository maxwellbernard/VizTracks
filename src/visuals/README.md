# Visuals package

This folder contains the visualization code split by concern. The public API is re-exported at `src/visuals/__init__.py` so callers can import stable names regardless of internal layout.

## Structure

- core/
  - constants.py — default visualization constants (days, dpi, figsize, interp_steps, period, RESAMPLING_FILTER)
  - cache.py — shared caches (image_cache, color_cache) and error tracking (error_logged)
  - colors.py — color utilities (get_dominant_color)
  - fonts.py — font loading (get_fonts)
  - style.py — plot styling helpers (setup_bar_plot_style)
- io/
  - images.py — Spotify image fetch and batching (fetch_images_batch)
- plots/
  - create_bar_plot.py — static bar plot figure generation
- anims/
  - create_bar_animation.py — bar chart race animation generator (re-exports default constants for convenience)
  (Legacy shim `prepare_visuals.py` has been removed; import from `src.visuals` instead.)

## Public API (import from src.visuals)

- Constants: `days`, `dpi`, `figsize`, `interp_steps`, `period`, `RESAMPLING_FILTER`
- Caches: `image_cache`, `color_cache`, `error_logged`
- Helpers: `get_fonts`, `get_dominant_color`, `setup_bar_plot_style`, `fetch_images_batch`

Example:

```python
from src.visuals import days, figsize, image_cache, get_fonts, setup_bar_plot_style
```

## Notes

- Prefer importing from `src.visuals` (public API) instead of deep module paths; this keeps call sites stable if internals move.
  The old `prepare_visuals.py` shim has been removed. Use the `src.visuals` public API.
