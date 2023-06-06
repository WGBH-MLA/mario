FROM python

WORKDIR /app

COPY pyproject.toml README.md ./
COPY mario mario

RUN pip install . rich


CMD ["rq", "worker", "-u", "redis://redis/0"]