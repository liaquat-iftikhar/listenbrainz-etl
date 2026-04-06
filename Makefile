# Variables
PYTHON ?= python
VENV_DIR := .venv
DOCKER_IMAGE_NAME := listenbrainz-etl-etl

# Platform-specific paths
ifeq ($(OS),Windows_NT)
    PIP := $(VENV_DIR)\Scripts\pip.exe
    PYTHON_VENV := $(VENV_DIR)\Scripts\python.exe
    FLake8 := $(VENV_DIR)\Scripts\flake8.exe
    RM := rmdir /s /q
    DEL := del /q
    TEST_INTEGRATION := set PYSPARK_PYTHON=$(PYTHON_VENV) && set PYSPARK_DRIVER_PYTHON=$(PYTHON_VENV) && pytest tests\integration
    TEST_UNIT := set PYSPARK_PYTHON=$(PYTHON_VENV) && set PYSPARK_DRIVER_PYTHON=$(PYTHON_VENV) && pytest tests\unit
else
    PIP := $(VENV_DIR)/bin/pip
    PYTHON_VENV := $(VENV_DIR)/bin/python
    FLake8 := $(VENV_DIR)/bin/flake8
    RM := rm -rf
    DEL := rm -f
    TEST_INTEGRATION := PYSPARK_PYTHON=$(PYTHON_VENV) PYSPARK_DRIVER_PYTHON=$(PYTHON_VENV) pytest tests/integration
    TEST_UNIT := PYSPARK_PYTHON=$(PYTHON_VENV) PYSPARK_DRIVER_PYTHON=$(PYTHON_VENV) pytest tests/unit
endif

# Default target
.PHONY: help
help:
	@echo "Usage:"
	@echo "  make setup           - Set up the virtual environment and install dependencies"
	@echo "  make run             - Run the ETL pipeline"
	@echo "  make run-sql-reports - Run the SQL reports"
	@echo "  make lint            - Run linting (flake8)"
	@echo "  make test            - Run unit and integration tests with pytest"
	@echo "  make docker-build    - Build the Docker image"
	@echo "  make docker-run      - Run the ETL pipeline in Docker"
	@echo "  make clean           - Remove temporary files"
	@echo "  make clean-docker    - Remove docker image"

.PHONY: setup
setup:
	@echo "Setting up virtual environment..."
	$(PYTHON) -m venv $(VENV_DIR)
	$(PYTHON_VENV) -m pip install --upgrade pip
	$(PIP) install -r requirements.txt

.PHONY: run
run:
	@echo "Running ETL pipeline..."
	$(PYTHON_VENV) main.py

.PHONY: run-sql-reports
run-sql-reports:
	@echo "Running SQL reports..."
	$(PYTHON_VENV) sql_executor.py

.PHONY: lint
lint:
	@echo "Running flake8 linting..."
	$(FLake8) etl

.PHONY: test
test:
	@echo "Running integration tests with pytest..."
	$(TEST_INTEGRATION)

	@echo "Running unit tests with pytest..."
	$(TEST_UNIT)

.PHONY: docker-build
docker-build:
	@echo "Building Docker image..."
	docker build -t etl-pipeline .

.PHONY: docker-run
docker-run:
	@echo "Running ETL pipeline in Docker..."
	docker compose up

.PHONY: clean
clean:
	@echo "Cleaning up..."
ifeq ($(OS),Windows_NT)
	@if exist __pycache__ $(RM) __pycache__
	@if exist .pytest_cache $(RM) .pytest_cache
	@if exist .mypy_cache $(RM) .mypy_cache
	@if exist $(VENV_DIR) $(RM) $(VENV_DIR)
	@if exist output ( \
		$(DEL) output\*.* && \
		for /d %%x in (output\*) do $(RM) "%%x" \
	)
else
	$(RM) __pycache__ .pytest_cache .mypy_cache $(VENV_DIR) output/*
	find . -type d -name "__pycache__" -exec rm -r {} +
endif

.PHONY: clean-docker
clean-docker:
	@echo "Removing Docker images for this project..."
	docker rmi -f $(DOCKER_IMAGE_NAME) || true
