## GPU/NVENC-enabled base with all ffmpeg libs present
FROM jrottenberg/ffmpeg:6.1-nvidia

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
	ca-certificates \
	python3 python3-pip \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /code

COPY encoder/requirements.txt /code/requirements.txt
RUN pip3 install --no-cache-dir -r /code/requirements.txt

COPY encoder /code/encoder

ENV PORT=8080
EXPOSE 8080


ENTRYPOINT []
CMD ["gunicorn", "-w", "1", "-k", "sync", "--threads", "1", "--timeout", "1200", "--graceful-timeout", "1200", "--keep-alive", "5", "--no-sendfile", "-b", "0.0.0.0:8080", "encoder.app:app"]
