import os
import time

import psutil

from backend.core.config import UPLOAD_DIR

os.makedirs(UPLOAD_DIR, exist_ok=True)


def log_mem(msg: str):
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024**2
    print(f"{msg} - Memory usage: {mem_mb:.2f} MB")


def cleanup_old_sessions(upload_dir: str = UPLOAD_DIR, max_age_seconds: int = 2700):
    now = time.time()
    for fname in os.listdir(upload_dir):
        fpath = os.path.join(upload_dir, fname)
        if os.path.isfile(fpath) and (now - os.path.getmtime(fpath)) > max_age_seconds:
            try:
                os.remove(fpath)
            except Exception:
                pass
