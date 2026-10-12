# Convenience targets for the docker compose stack.
# Override the project name with `make up COMPOSE_PROJECT_NAME=myname`.
COMPOSE_PROJECT_NAME ?= streamanon
export COMPOSE_PROJECT_NAME

COMPOSE := docker compose
PYTHON ?= python3

.PHONY: up down clean logs ps smoke-test build synthetic-v2

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

# Same arguments as in load-generator/Dockerfile, which generates the set at image build time.
synthetic-v2: ## Generate the synthetic ICU data set (schema v2) in load-generator/output_json_files_v2 (needs faker)
	rm -rf load-generator/output_json_files_v2
	cd load-generator && $(PYTHON) generateData.py --schema v2 --num-users 10 --num-timestamps 600 \
		--interval-minutes 1 --seed 2 --output-dir output_json_files_v2
