FROM python:3.11-slim

WORKDIR /app

# Копируем код раннера (всё содержимое runner/)
COPY runner /app/runner
COPY pyproject.toml /app/runner/
COPY poetry.lock /app/runner/
COPY ../internal/config /app/internal/config

WORKDIR /app/runner

RUN pip install --upgrade pip && pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root

EXPOSE 8003

CMD ["poetry", "run", "python", "main.py"]
