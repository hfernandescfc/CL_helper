CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.ingestion_watermarks (
  entity TEXT PRIMARY KEY,
  watermark TIMESTAMP,
  updated_at TIMESTAMP DEFAULT now()
);


CREATE TABLE IF NOT EXISTS meta.ingestion_watermarks_v2 (
  source TEXT,
  entity TEXT,
  key TEXT,
  last_success_at TIMESTAMP,
  high_watermark TIMESTAMP,
  updated_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY (source, entity, key)
);

CREATE TABLE IF NOT EXISTS meta.load_audit (
  run_id UUID,
  flow TEXT,
  task TEXT,
  target TEXT,
  rows_inserted BIGINT,
  rows_updated BIGINT,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  status TEXT,
  message TEXT
);

