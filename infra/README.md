# Infra layout

This repository separates backend and frontend deployment configs:

```
infra/
  backend/
    Dockerfile        # Flask backend (Gunicorn on 8080)
    fly.toml          # Fly config for backend app
  frontend/
    Dockerfile        # Streamlit frontend (8501)
    fly.toml          # Fly config for frontend app
  .dockerignore       # Shared ignore for Docker build contexts
```

Notes
- Keep the top-level `.dockerignore`; it applies when building from repo root.
- If you later build from subfolders directly, consider adding per-folder `.dockerignore` files too.
- Update deploy commands to point to the subfolder toml files.
# Infra (Fly.io)

This folder contains the deployment configuration for Fly.io.

## Files

- fly.toml — Fly app configuration (app name, region, service, VM size, etc.).
- Dockerfile — Build image for the backend service.
- .dockerignore — Exclude unneeded files from the build context.

## Deploy

From the repo root, point `flyctl` at this toml (since it's not in the root):

```pwsh
flyctl deploy --remote-only -c infra/fly.toml -a spotify-animation
```

Alternatively, from inside `infra/`:

```pwsh
Set-Location infra
flyctl deploy --remote-only -a spotify-animation
```

Notes:
- `app` is set in `infra/fly.toml` (spotify-animation). Passing `-a` is still fine and explicit.
- The Dockerfile is co-located here; running deploy from this folder or with `-c infra/fly.toml` ensures Fly finds it.

## Runtime

- HTTP service listens on port 8080 (see `[http_service]` in `fly.toml`).
- VM config is set to 4 CPUs / 16GB in `[[vm]]`.
- Auto-start/stop is enabled for cost control (`auto_start_machines`, `auto_stop_machines`).

## CI usage

In GitHub Actions or other CI, call:

```pwsh
flyctl deploy --remote-only -c infra/fly.toml -a spotify-animation
```

Make sure Fly API token is configured (secrets/ENV): `FLY_API_TOKEN`.
