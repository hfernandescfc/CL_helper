# FootballData Pipeline (DuckDB + Prefect)

Pipeline de dados com camadas bronze/silver/gold, orquestrado com Prefect e armazenado em DuckDB. Saída para notebooks e Streamlit.

## Requisitos
- Python 3.10+
- Dependências via `pyproject.toml`

## Setup
1. Crie ambiente e instale deps:
   - `pip install -e .[dev]`
2. Copie `.env.example` para `.env` e preencha `FOOTBALL_DATA_API_KEY`.
3. Inicialize schemas/metadata:
   - `python -c "import duckdb; con=duckdb.connect('warehouse/warehouse.duckdb'); con.execute(open('sql/utils/00_init.sql').read()); print('OK')"`

## Executar Flow Diário
- `python flows/daily_etl.py`

## Estrutura
- `src/footballdata`: pacote com config, IO (DuckDB), extratores, transformações e métricas
- `sql/`: transformações SQL para `silver/` e `gold/`
- `flows/`: flows Prefect para ETL diário, backfill e refresh de métricas
- `app/`: app Streamlit de exemplo
- `warehouse/`: arquivo DuckDB
- `data/raw/`: dumps opcionais de extrações

## Notas
- Queries de consumo (notebooks/Streamlit) devem ler preferencialmente de `gold`.
- O flow usa watermark em `meta.ingestion_watermarks`.

