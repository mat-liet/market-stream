COMPOSE := docker compose -f infra/docker-compose.yml --project-name market-stream
PROFILES := --profile core --profile observability

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------- stack ----

.PHONY: up
up: ## Start the full stack (core + observability)
	$(COMPOSE) $(PROFILES) up -d
	@echo
	@echo "  Redpanda Console  http://localhost:8085"
	@echo "  Schema Registry   http://localhost:8081"
	@echo "  ClickHouse        http://localhost:8123"
	@echo "  Prometheus        http://localhost:9090"
	@echo "  Grafana           http://localhost:3000"

.PHONY: core
core: ## Start only the data path (no Prometheus/Grafana)
	$(COMPOSE) --profile core up -d

.PHONY: down
down: ## Stop the stack, keeping volumes
	$(COMPOSE) $(PROFILES) down

.PHONY: reset
reset: ## Stop the stack and wipe all volumes (Kafka, ClickHouse, Postgres)
	$(COMPOSE) $(PROFILES) down -v

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) $(PROFILES) ps

.PHONY: logs
logs: ## Tail logs for all services (make logs SERVICE=kafka for one)
	$(COMPOSE) $(PROFILES) logs -f $(SERVICE)

# ------------------------------------------------------------- inspection ----

.PHONY: topics
topics: ## List topics with partition count and config
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:29092 --describe

.PHONY: lag
lag: ## Show consumer lag for every group
	$(COMPOSE) exec kafka bash -c '\
		for g in $$(kafka-consumer-groups --bootstrap-server kafka:29092 --list); do \
			echo "== $$g"; \
			kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "$$g"; \
		done'

.PHONY: migrate
migrate: ## Re-apply Postgres and ClickHouse migrations
	$(COMPOSE) --profile core up flyway clickhouse-init

.PHONY: psql
psql: ## Open a psql shell on the config store
	$(COMPOSE) exec postgres psql -U market -d market

.PHONY: clickhouse
clickhouse: ## Open a clickhouse-client shell
	$(COMPOSE) exec clickhouse clickhouse-client --user market --password $${CLICKHOUSE_PASSWORD:-market} --database market

# ------------------------------------------------------------------ build ----

.PHONY: build
build: ## Compile all modules and run unit tests
	mvn -q verify

.PHONY: clean
clean: ## Remove build output
	mvn -q clean
