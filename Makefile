# Fleet Lambda — developer commands. Compose project name is "fleet" (docker-compose.yml),
# so every command below only touches this project's containers, networks and volumes.

COMPOSE     ?= docker compose
RUFF        ?= $(if $(wildcard .venv/bin/ruff),.venv/bin/ruff,ruff)
RUN         = $(COMPOSE) run --rm runner
SMOKE_TOPIC ?= infra.smoke
KAFKA_CLI   = $(COMPOSE) exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka
SMOKE_CLEANUP = $(KAFKA_CLI) kafka-topics --bootstrap-server kafka:9092 --delete --topic $(SMOKE_TOPIC) --if-exists

.PHONY: help env dirs build up up-app down reset ps logs psql lint format \
        test-unit test-integration test smoke smoke-topic smoke-airflow

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

env: ## create .env from .env.example (only if missing)
	@if [ -f .env ]; then echo ".env already exists, leaving it untouched"; \
	else cp .env.example .env && echo "created .env from .env.example - change the passwords"; fi

dirs:
	@mkdir -p data/landing/expenses

build: env dirs ## build the runtime and airflow images
	$(COMPOSE) --profile app --profile tools build

up: env dirs ## start infrastructure: postgres, kafka, airflow
	$(COMPOSE) up -d --wait

up-app: env dirs ## start infrastructure + application services (profile app)
	$(COMPOSE) --profile app up -d

down: env ## stop and remove this project's containers (volumes are kept)
	$(COMPOSE) --profile app --profile tools down

reset: env ## stop this project AND delete its volumes (postgres, kafka, airflow logs, checkpoints) + landing files
	@echo "Removing containers and volumes of compose project 'fleet' only (DB and Kafka data will be lost)"
	$(COMPOSE) --profile app --profile tools down -v --remove-orphans
	@echo "Removing generated landing files in data/landing/expenses/"
	rm -rf data/landing/expenses/*

ps: env ## show service status
	$(COMPOSE) --profile app --profile tools ps

logs: env ## follow logs (make logs S=airflow for one service)
	$(COMPOSE) --profile app logs -f --tail=200 $(S)

psql: env ## psql shell on the fleet database
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

lint: ## ruff check + format check
	$(RUFF) check .
	$(RUFF) format --check .

format: ## apply ruff fixes and formatting
	$(RUFF) check --fix .
	$(RUFF) format .

test-unit: ## unit tests inside the runner container
	$(RUN) pytest -m "not integration"

test-integration: ## integration tests (needs `make up`)
	$(RUN) pytest -m integration

test: ## full test suite
	$(RUN) pytest

smoke-topic:
	$(KAFKA_CLI) kafka-topics --bootstrap-server kafka:9092 --delete --topic $(SMOKE_TOPIC) --if-exists
	$(KAFKA_CLI) kafka-topics --bootstrap-server kafka:9092 --create --topic $(SMOKE_TOPIC) --partitions 1 --replication-factor 1
	printf 'smoke-1\nsmoke-2\nsmoke-3\n' | $(KAFKA_CLI) kafka-console-producer --bootstrap-server kafka:9092 --topic $(SMOKE_TOPIC)

smoke: smoke-topic ## Spark smoke test in fleet-runtime: Kafka connector + JDBC (throwaway topic/table)
	$(RUN) python docker/smoke/spark_smoke.py --topic $(SMOKE_TOPIC) --expected 3; \
	  rc=$$?; $(SMOKE_CLEANUP); exit $$rc

smoke-airflow: smoke-topic ## same Spark Kafka + JDBC check inside the running airflow container
	$(COMPOSE) exec -T airflow spark-submit --version 2>&1 | grep -E 'version [0-9]'
	$(COMPOSE) exec -T airflow python - --topic $(SMOKE_TOPIC) --expected 3 < docker/smoke/spark_smoke.py; \
	  rc=$$?; $(SMOKE_CLEANUP); exit $$rc
