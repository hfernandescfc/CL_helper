# CL_helper

End-to-end data pipeline for football analytics built with Python, Prefect, DuckDB, and a bronze/silver/gold warehouse architecture.

## Overview
This project extracts match and odds data from external APIs, loads the data into DuckDB, applies layered transformations, and exposes curated datasets for analytics, notebooks, and a Streamlit app.

## Highlights
- Automated ETL/ELT workflows with Prefect
- Bronze, silver, and gold data modeling
- Incremental ingestion with watermarks
- Analytical metrics generation
- Streamlit app for exploration
- Test structure for core components

## Tech Stack
Python, DuckDB, Prefect, Pandas, PyArrow, SQL, Streamlit, Plotly

## Architecture
Data Sources -> Extraction -> Bronze -> Silver -> Gold -> Analytics / App

## Why this project matters
This repository demonstrates practical data engineering skills: orchestration, data modeling, idempotent ingestion, analytical serving layers, and reproducible local execution.
