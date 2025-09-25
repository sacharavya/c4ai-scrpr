PYTHON := python
PIP := $(PYTHON) -m pip
VENV := .venv
ACTIVATE := . $(VENV)/bin/activate

.PHONY: venv install lint type test cov fmt run docker

venv:
	@test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	@$(ACTIVATE) && $(PIP) install --upgrade pip
	@$(ACTIVATE) && $(PIP) install -e .[dev]

lint:
	@$(ACTIVATE) && ruff check .
	@$(ACTIVATE) && black --check .

type:
	@$(ACTIVATE) && mypy app scripts

test:
	@$(ACTIVATE) && pytest -q

cov:
	@$(ACTIVATE) && pytest --cov=app --cov-report=term-missing --cov-fail-under=85

fmt:
	@$(ACTIVATE) && black .
	@$(ACTIVATE) && ruff check --fix .

run:
	@$(ACTIVATE) && python -m app.main crawl --type events --limit 25

docker:
	docker build -t c4ai-scrpr .
