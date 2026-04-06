# ListenBrainz ETL Pipeline

A PySpark-based ETL pipeline to extract, transform, and load music listening history data (JSON) into DuckDB for analytical querying.

## Features

- Clean ETL architecture (Extract, Transform, Load)
- Spark-based schema enforcement and transformations
- DuckDB for lightweight, high-performance analytics
- Unit and integration test coverage
- Docker support for isolated environment setup

---

## Project Structure
```
listenbrainz-etl/
├── etl/
│ ├── config.py                          # Global configs and constants
│ ├── extract/
│ │ ├── reader.py                        # JSON file reader using Spark
│ │ └── schema.py                        # Spark schema definition
│ ├── transform/
│ │ ├── flatten_transformer.py           # Flattens nested JSON/struct fields
│ │ ├── listen_event_data_transformer    # Table-specific tranformer
│ │ └── ...
│ ├── load/
│ │ ├── base_loader.py                   # Generic loader to DuckDB
│ │ ├── user_listens_loader.py           # Table-specific loader
│ │ └── ...
│ ├── utils/
│ │ ├── duck_db_connector.py             # Manages DuckDB connections
│ │ └── duck_db_writer.py                # Handles deduplication & writing logic
├── sql/                                 # Contains the SQLs for the report
│ ├── 1_top_users.sql                    # SQL for Top 10 users by number of songs listened to
│ └── ...
├── tests/
│ ├── unit/                              # Isolated logic tests
│ └── integration/                       # End-to-end + database tests
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── main.py
├── Makefile
├── README.md
├── requirements.txt
└── sql_executor.py
```

---

## Getting Started

## Prerequisites
- Python 3.11 or above
- Make (comes pre-installed on Linux/macOS, install via GnuWin32 or WSL on Windows)
- Docker & Docker Compose
- Java 17 (for PySpark)

## Project Setup

This project uses `Makefile` for setup, testing, and Dockerization. Follow the steps below to get started:

---

### 1. Set Up Python Virtual Environment

Create a virtual environment and install all dependencies:

```bash
make setup
```

This will:

* Create a `.venv/` folder.
* Upgrade `pip`.
* Install all required Python packages listed in `requirements.txt`.

---

### 2. Run the ETL Pipeline Locally

To run the main ETL process:

```bash
make run
```

---

### 3. Run Linting (Optional but Recommended)

Check code style using `flake8`:

```bash
make lint
```

---

### 4. Run Tests

This project separates tests into **unit** and **integration** folders. 
#### Activating the Virtual Environment

Before running the tests, make sure to activate the Python virtual environment.

##### For Windows:

```bash
.\venv\Scripts\activate
```
##### For Mac OS:
```bash
source venv/bin/activate
```
#### Executing Test:

To run both:

```bash
make test
```

You can also run them individually:

```bash
# Unit tests
PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python pytest tests/unit

# Integration tests
PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python pytest tests/integration
```

> On Windows, these commands are automatically adjusted for your environment via `Makefile`.

---

### 5. Use Docker (Optional)

Build a Docker image:

```bash
make docker-build
```

Run the ETL inside Docker:

```bash
make docker-run
```

---

### 6. Running SQL Reports

To run the SQL reports using the provided Makefile target, use the following command:

```bash
make run-sql-reports
```

This command will:
- Execute the sql_runner.py script, which runs all predefined SQL queries and displays the results.

### 7. Clean Up

Remove temporary files and virtual environment:

```bash
make clean
```

### 8. Remove the Docker image:

```bash
make clean-docker
```
