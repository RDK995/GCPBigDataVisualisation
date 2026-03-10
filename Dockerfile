FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir poetry==1.8.3

COPY pyproject.toml README.md ./
COPY src ./src

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --without dev

ENTRYPOINT ["python", "-m", "src.main"]
