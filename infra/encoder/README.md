Encoder (GPU NVENC microservice)

Overview
- Small Flask app that encodes a sequence of PNG frames into H.264 MP4 using FFmpeg (NVENC).
- Deployed as a separate Fly.io app so it can scale and use GPUs independently of the backend.

Endpoints
- GET / → 200 OK (simple liveness)
- GET /health → { status: "ok" }
- POST /encode
  - Request JSON: { fps: number, frames: string[] } where frames are base64-encoded PNG images in order
  - Response (200): { video: string } where video is base64-encoded MP4
  - Errors (500/400): { error: string, detail?: string }

Notes
- The service is NVENC-only: it checks ffmpeg -hwaccels and fails if CUDA/NVENC isn’t available.
- Output is mobile-safe H.264: yuv420p, +faststart, GOP ~ 2×fps, no audio.
