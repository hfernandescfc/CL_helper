# FootballData Pipeline (DuckDB + Prefect)

End-to-end ETL/ELT pipeline for football data. It extracts match and odds data from Football-Data.org and The Odds API, loads it into DuckDB, and builds bronze/silver/gold layers for analytics, notebooks, and a Streamlit demo app.

## Features
- Automated daily ETL with Prefect flows
- DuckDB warehouse with bronze/silver/gold layers
- Optional backfill and metrics refresh flows
- Streamlit app example for data exploration
- Idempotent loading with watermarks

## Data Sources
- Football-Data.org API
- The Odds API

## Tech Stack
- Python 3.10+
- Prefect 2.x
- DuckDB
- Pandas, PyArrow
- Streamlit, Plotly

## Project Structure
- `src/footballdata/`: core package (config, extract, transform, metrics, utils)
- `flows/`: Prefect flows (daily, backfill, refresh)
- `sql/`: SQL transformations for silver and gold
- `warehouse/`: DuckDB database file
- `app/`: Streamlit app
- `data/raw/`: optional raw dumps

## Quickstart
1. Create a virtual env and install deps:
   ```bash
   pip install -e .[dev]
   ```
2. Copy env template:
   ```bash
   cp .env.example .env
   ```
3. Set your API keys in `.env`:
   - `FOOTBALL_DATA_API_KEY`
   - `ODDS_API_KEY`
4. Initialize the DuckDB warehouse:
   ```bash
   python -c "import duckdb; con=duckdb.connect('warehouse/warehouse.duckdb'); con.execute(open('sql/utils/00_init.sql').read()); print('OK')"
   ```

## Run the Daily Flow
```bash
python flows/daily_etl.py
```

## Backfill and Metrics
```bash
python flows/backfill.py
python flows/refresh_metrics.py
```

## Streamlit App
```bash
streamlit run app/streamlit_app.py
```

## Notes
- Analytical queries should read from `gold` tables when possible.
- The pipeline uses watermarks in `meta.ingestion_watermarks` for idempotency.
