# GamePricesDWH

Data Warehouse for tracking game prices across multiple stores using the ITAD (IsThereAnyDeal) API.

## Architecture
```
ITAD API → PostgreSQL (raw) → ClickHouse (DWH) → Apache Superset (BI)
                                    ↑
                              dbt (transforms)
                                    ↑
                            Apache Airflow (orchestration)
```

## Stack

| Component | Technology |
|-----------|-----------|
| Data Source | ITAD API (27+ stores) |
| Raw Storage | PostgreSQL 15 |
| Data Warehouse | ClickHouse 24.8 |
| Transformations | dbt 1.8 + dbt-clickhouse |
| Orchestration | Apache Airflow 2.x |
| BI / Dashboards | Apache Superset |
| Infrastructure | Docker Compose |

## Data Models

### Staging (`default_staging`)
- `stg_games` — games with developer and publisher names
- `stg_prices` — cleaned and normalized prices
- `stg_stores` — store names

### Marts (`default_marts`)
- `dm_prices_latest` — latest price snapshot per game per store
- `dm_best_deals` — best discount available per game
- `dm_store_stats` — aggregated statistics per store
- `dm_price_history` — full price history partitioned by month

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.12+

### 1. Clone the repository
```bash
git clone https://github.com/Spikew285/GamePricesDWH.git
cd GamePricesDWH
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Start all services
```bash
docker compose up -d
```

### 4. Run dbt models
```bash
cd dbt
pip install dbt-core==1.8.7 dbt-clickhouse==1.8.1
dbt run
dbt test
```

### 5. Access services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | see .env |
| Superset | http://localhost:8088 | see .env |
| ClickHouse | http://localhost:8123 | default / (no password) |

## Project Structure
```
GamePricesDWH/
├── airflow/
│   ├── dags/ingest_pipeline.py   # Main DAG
│   ├── entrypoint.sh
│   └── requirements.txt
├── dbt/
│   ├── models/
│   │   ├── staging/              # Staging models
│   │   └── marts/                # Data mart models
│   └── dbt_project.yml
├── ingestion/
│   ├── itad_api_loader.py        # ITAD API client
│   └── pg_to_clickhouse.py       # PG → ClickHouse sync
├── warehouse/
│   ├── postgres_schema.sql       # Raw layer schema
│   └── clickhouse_schema.sql     # DWH schema
├── superset/
│   ├── Dockerfile
│   ├── init.sh
│   └── superset_config.py
├── clickhouse/
│   └── user_config.xml
├── docker-compose.yml
└── requirements.txt
```

## dbt Tests

72 tests covering:
- not_null constraints
- unique keys
- referential integrity (FK relationships)
- accepted values (currency codes)

Run tests:
```bash
dbt test
```

## Notes

- ClickHouse 24.8 new query analyzer requires CTE-based joins (subquery aliases not resolved)
- dbt models use `table` materialization (not `view`) for ClickHouse compatibility
- Superset requires `clickhouse-connect` installed in `/app/.venv/` — handled via custom Dockerfile