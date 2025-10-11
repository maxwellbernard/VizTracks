ARG PYTHON_VERSION=3.13.5

FROM python:${PYTHON_VERSION}-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /code

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /code/
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . /code

EXPOSE 8501

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["streamlit", "run", "app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
