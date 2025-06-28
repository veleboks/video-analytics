FROM python:3.11-slim

WORKDIR /app

# Установка poetry и ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && \
    pip install --no-cache-dir poetry

COPY pyproject.toml ./
COPY poetry.lock ./
COPY internal ./internal

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

EXPOSE 8000

CMD ["uvicorn", "internal.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
