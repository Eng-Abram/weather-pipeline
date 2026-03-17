# Weather Pipeline 🌤️

An end-to-end ELT data pipeline that fetches daily weather data from a public REST API, transforms it using dbt, and orchestrates the entire workflow with Apache Airflow — all running on GitHub Codespaces.

---

## Architecture

```
Open-Meteo API → Python (Extract & Load) → DuckDB
     → dbt (Staging → Mart) → Airflow DAG (Daily Schedule)
```

| Layer | Tool | Role |
|---|---|---|
| Extract & Load | Python + Requests | Calls the API, parses JSON, loads raw rows into DuckDB |
| Storage | DuckDB | Lightweight local database — no server needed |
| Transform | dbt (dbt-duckdb) | Cleans, types, and aggregates raw data into analytical models |
| Orchestrate | Apache Airflow | Schedules and runs the full pipeline daily |

---

## Data Source

**[Open-Meteo](https://open-meteo.com/)** — free, no API key required.

Fetches the last 61 days + next 7 days of daily weather for Cairo, Egypt (lat: 30.06, lon: 31.25), including:

- Max / min temperature (°F)
- Sunrise & sunset times
- Max wind speed (km/h)
- WMO weather code

---

## Project Structure

```
weather-pipeline/
├── dags/
│   └── weather_dag.py              # Airflow DAG — 3 tasks in sequence
├── scripts/
│   └── fetch_weather.py            # Calls API, loads raw data to DuckDB
├── weather_dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── sources.yml             # Declares raw DuckDB table as dbt source
│   │   ├── staging/
│   │   │   ├── stg_weather.sql     # Cleans and types raw data
│   │   │   └── schema.yml          # Column tests
│   │   └── marts/
│   │       ├── mart_weather_daily.sql   # Weekly aggregated summaries
│   │       └── schema.yml
├── weather.duckdb                  # Auto-created by the extraction script
└── requirements.txt
```

---

## Pipeline Flow

The Airflow DAG runs three tasks in order:

```
fetch_1  →  dbt_run_1  →  dbt_test_1
```

**Task 1 — `fetch_1`**
Runs `fetch_weather.py` which calls the Open-Meteo API, parses the JSON response, and loads rows into the `weather_raw` table in DuckDB. Deletes today's existing rows before inserting to ensure idempotency.

**Task 2 — `dbt_run_1`**
Runs `dbt run` to execute all models:
- `stg_weather` (view) — renames columns, casts types, converts F→C, adds weather description
- `mart_weather_daily` (table) — weekly aggregations: avg/max/min temps, total rain, wind averages

**Task 3 — `dbt_test_1`**
Runs `dbt test` to validate all schema tests (not_null, unique) against the final models.

---

## dbt Models

### `stg_weather` (staging — view)

Reads from `{{ source('raw', 'weather_raw') }}` — the table loaded externally by Python.

Transformations applied:
- Cast `date` string → `DATE`
- Cast `sunrise` / `sunset` → `TIMESTAMP`
- Convert temperatures from Fahrenheit to Celsius
- Calculate `temp_range_c`
- Map `weather_code` integer → human-readable `weather_state`

### `mart_weather_daily` (mart — table)

Reads from `{{ ref('stg_weather') }}`.

Aggregates daily rows into weekly summaries:
- Average, max, and min temperatures
- Total precipitation
- Average wind speed
- Rain category (Rainy / Some rain / Dry)

---

## Key Concepts Demonstrated

**`source()` vs `ref()` in dbt**

| Function | When to use |
|---|---|
| `{{ source('raw', 'weather_raw') }}` | Table was loaded externally (by Python) |
| `{{ ref('stg_weather') }}` | Table was created by another dbt model |

**Idempotency in the extraction script**

```python
# Deletes today's rows before re-inserting — safe to re-run multiple times
conn.execute("DELETE FROM weather_raw WHERE loaded_at::DATE = CURRENT_DATE")
```

**ELT vs ETL**

This project follows the ELT pattern — raw data lands in the database first (via Python), then transformations happen inside the database (via dbt). dbt never touches the API.

---

## Setup & Running

### Prerequisites

- GitHub Codespaces (or any Linux environment)
- Python 3.11+

### Installation

```bash
# Clone the repo
git clone https://github.com/Eng-Abram/weather-pipeline.git
cd weather-pipeline

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Configure dbt

Create `~/.dbt/profiles.yml`:

```yaml
weather_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /workspaces/weather-pipeline/weather_dbt/weather.duckdb
      threads: 1
```

### Run manually (without Airflow)

```bash
# Step 1 — Extract and load raw data
python scripts/fetch_weather.py

# Step 2 — Run dbt models
cd weather_dbt
dbt run

# Step 3 — Test dbt models
dbt test
```

### Run with Airflow

```bash
# Set environment variables
export AIRFLOW_HOME=/home/vscode/airflow
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8080

# Point Airflow at the dags folder
sed -i 's|^dags_folder.*|dags_folder = /workspaces/weather-pipeline/dags|' ~/airflow/airflow.cfg

# Initialize and start
airflow db migrate
airflow standalone
```

Open the Airflow UI at `http://localhost:8080`, log in, enable the `weather_update_1` DAG, and trigger it manually or let the `*/10 * * * *` schedule handle it.

---

## Requirements

```
apache-airflow
dbt-duckdb
duckdb
requests
```

---

## Lessons Learned

- **APIs + dbt**: dbt does not call APIs. Python handles Extract & Load; dbt handles Transform only. Use `source()` in dbt to reference externally loaded tables.
- **DuckDB connections**: Always close the connection (`conn.close()` or use a `with` block) to avoid file corruption.
- **Airflow on Codespaces**: Set `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8080` explicitly to prevent the scheduler from routing internal HTTP calls through the public Codespaces URL.
- **Idempotency**: A pipeline that can be safely re-run multiple times without corrupting data is a production requirement, not optional.

---

## Author

**Abram** — Data Engineering student, DEPI program.
