FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=1.8.3 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

COPY pyproject.toml README.md ./

FROM base AS runtime

COPY src ./src

RUN poetry install --no-interaction --no-ansi --without dev

ENTRYPOINT ["python", "-m", "src.main"]

FROM base AS dev

COPY src ./src
COPY tests ./tests

RUN poetry install --no-interaction --no-ansi

ENTRYPOINT ["poetry", "run"]
