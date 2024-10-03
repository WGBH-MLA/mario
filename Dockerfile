FROM python:3.12-slim
RUN apt update && \
    apt install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY mario mario
RUN pip install .[whisper]

ENTRYPOINT [ "/app/mario/pipelines/runner.py"]
