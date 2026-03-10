.PHONY: install lint test run docker-build

install:
	poetry install

lint:
	poetry run ruff check src tests
	poetry run mypy src

test:
	poetry run pytest

run:
	poetry run python -m src.main

docker-build:
	docker build -t gcp-bigdata-visualisation:local .
