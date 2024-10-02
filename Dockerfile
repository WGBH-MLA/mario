FROM python:3.12-slim
RUN apt update && apt install -y git ffmpeg
RUN pip install -U pip
RUN pip install git+https://github.com/openai/whisper.git

WORKDIR /app
COPY pyproject.toml README.md ./
COPY mario mario
RUN pip install .[whisper]

ENTRYPOINT [ "/app/mario/pipelines/runner.py"]
