.PHONY: install lint test run docker-build docker-build-dev docker-lint docker-test docker-run deploy metabase-up metabase-down

GOOGLE_APPLICATION_CREDENTIALS_FILE ?= $(HOME)/.config/gcloud/application_default_credentials.json
METABASE_ENCRYPTION_SECRET_KEY ?= change-this-metabase-secret-key-before-sharing

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
	docker build --target runtime -t gcp-bigdata-visualisation:local .

docker-build-dev:
	docker build --target dev -t gcp-bigdata-visualisation:dev .

docker-lint:
	docker build --target dev -t gcp-bigdata-visualisation:dev .
	docker run --rm gcp-bigdata-visualisation:dev ruff check src tests
	docker run --rm gcp-bigdata-visualisation:dev mypy src

docker-test:
	docker build --target dev -t gcp-bigdata-visualisation:dev .
	docker run --rm gcp-bigdata-visualisation:dev pytest

docker-run:
	docker build --target runtime -t gcp-bigdata-visualisation:local .
	docker run --rm \
		--env-file .env \
		-e GOOGLE_CLOUD_PROJECT \
		-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-adc.json \
		-v "$(GOOGLE_APPLICATION_CREDENTIALS_FILE):/tmp/gcp-adc.json:ro" \
		gcp-bigdata-visualisation:local

deploy:
	gcloud builds submit --config infra/cloudbuild.yaml .

metabase-up:
	docker network inspect gcp-bigdata-metabase >/dev/null 2>&1 || docker network create gcp-bigdata-metabase
	docker volume inspect gcp-bigdata-metabase-data >/dev/null 2>&1 || docker volume create gcp-bigdata-metabase-data
	docker volume inspect gcp-bigdata-metabase-db-data >/dev/null 2>&1 || docker volume create gcp-bigdata-metabase-db-data
	docker rm -f gcp-bigdata-metabase-db >/dev/null 2>&1 || true
	docker rm -f gcp-bigdata-metabase >/dev/null 2>&1 || true
	docker run -d \
		--name gcp-bigdata-metabase-db \
		--network gcp-bigdata-metabase \
		-e POSTGRES_DB=metabase \
		-e POSTGRES_USER=metabase \
		-e POSTGRES_PASSWORD=metabase \
		-v gcp-bigdata-metabase-db-data:/var/lib/postgresql/data \
		postgres:16-alpine
	docker run -d \
		--name gcp-bigdata-metabase \
		--network gcp-bigdata-metabase \
		-p 8080:3000 \
		-e MB_DB_TYPE=postgres \
		-e MB_DB_DBNAME=metabase \
		-e MB_DB_PORT=5432 \
		-e MB_DB_USER=metabase \
		-e MB_DB_PASS=metabase \
		-e MB_DB_HOST=gcp-bigdata-metabase-db \
		-e MB_ENCRYPTION_SECRET_KEY=$(METABASE_ENCRYPTION_SECRET_KEY) \
		-v gcp-bigdata-metabase-data:/metabase-data \
		metabase/metabase:v0.56.5

metabase-down:
	docker rm -f gcp-bigdata-metabase >/dev/null 2>&1 || true
	docker rm -f gcp-bigdata-metabase-db >/dev/null 2>&1 || true
