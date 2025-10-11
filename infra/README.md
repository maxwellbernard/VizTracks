## Fly.io infrastructure

This repo deploys three Fly apps, each with its own Dockerfile and fly.toml:

```
infra/
  backend/   # Flask API (Gunicorn on 8080), region arn
  frontend/  # Streamlit UI (8501), region arn
  encoder/   # NVENC encoder microservice (8080), region ord, GPU L40S
```

### App details
- Backend
  - App: spotify-animation
  - Region: arn
  - Port: 8080
  - VM: performance, 4 CPUs, 16GB
- Frontend
  - App: spotify-animation-frontend
  - Region: arn
  - Port: 8501
  - VM: shared, 4 CPUs, 4GB
- Encoder (GPU)
  - App: spotify-animation-encoder
  - Region: ord
  - Port: 8080
  - VM: performance, gpu_kind=l40s, gpus=1, 4 CPUs, 16GB
