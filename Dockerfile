# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.13.5

FROM python:${PYTHON_VERSION}-slim

LABEL fly_launch_runtime="flask"

WORKDIR /code

COPY requirements.txt requirements.txt
RUN apt-get update && apt-get install -y ffmpeg
RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8080", "backend.process:app"]
