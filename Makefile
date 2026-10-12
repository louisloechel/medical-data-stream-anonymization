# Convenience targets for the docker compose stack.
# Override the project name with `make up COMPOSE_PROJECT_NAME=myname`.
COMPOSE_PROJECT_NAME ?= streamanon
export COMPOSE_PROJECT_NAME

COMPOSE := docker compose

.PHONY: up down clean logs ps smoke-test build

up: ## Build images and start the stack in the background
	$(COMPOSE) up --build -d

build: ## Build all images
	$(COMPOSE) build

down: ## Stop and remove containers (keeps Grafana/Prometheus volumes)
	$(COMPOSE) down

clean: ## Stop and remove containers and volumes
	$(COMPOSE) down -v

logs: ## Follow logs (e.g. `make logs S="flink-taskmanager og-prink-job"`)
	$(COMPOSE) logs -f --tail=100 $(S)

ps: ## Show container status
	$(COMPOSE) ps -a

smoke-test: ## End-to-end check of the running stack (exits non-zero on failure)
	./scripts/smoke-test.sh
