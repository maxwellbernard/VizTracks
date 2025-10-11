Encoder (GPU NVENC microservice)

Overview
- Flask app that encodes PNG frames to H.264 MP4 using FFmpeg (NVENC), deployed as a separate Fly.io app.

Endpoints
- GET / → 200 OK
- GET /health → { status: "ok" }
- POST /encode (JSON, legacy)
  - Body: { fps: number, frames: string[] } with base64 PNG frames
  - Response: { video: string } base64 MP4
- POST /encode_pipe (streaming, recommended)
  - Query: ?fps=number
  - Body: application/octet-stream, concatenated PNG images (image2pipe)
  - Response: { video: string } base64 MP4

Notes
- NVENC-only: ffmpeg -hwaccels must include cuda.
- Output: yuv420p, +faststart, GOP≈2×fps, no audio.
