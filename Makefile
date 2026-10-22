PY ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
DASH := dashboard

.PHONY: venv install install-ui dev test test-ui lint fmt build-ui worker dashboard bench seed openapi docker-up docker-down clean

venv:
	$(PY) -m venv $(VENV)

install: venv
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -e ".[dev]"

install-ui:
	cd $(DASH) && npm ci --no-audit --no-fund

dev: install install-ui

test:
	$(BIN)/pytest

test-ui:
	cd $(DASH) && npm test

lint:
	$(BIN)/ruff check src tests bench examples
	$(BIN)/ruff format --check src tests bench examples
	cd $(DASH) && npm run lint && npm run typecheck

fmt:
	$(BIN)/ruff format src tests bench examples
	$(BIN)/ruff check --fix src tests bench examples

build-ui:
	cd $(DASH) && npm run build

worker:
	$(BIN)/jobq worker --app examples.demo_app:app --queues high,default,low

dashboard:
	$(BIN)/jobq dashboard --app examples.demo_app:app --port 8000

seed:
	$(BIN)/python -m examples.demo_app

bench:
	$(BIN)/python bench/bench.py --jobs 20000

openapi:
	$(BIN)/python scripts/export_openapi.py

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

clean:
	rm -rf .pytest_cache .ruff_cache build dist *.egg-info src/*.egg-info $(DASH)/dist
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
