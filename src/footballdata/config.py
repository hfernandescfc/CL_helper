from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # External API
    FOOTBALL_DATA_API_KEY: str = "aeb49ceb34a242da96d56aeb92b7734a"
    FOOTBALL_DATA_BASE_URL: str = "https://api.football-data.org/v4"
    ODDS_API_KEY: str = ""
    ODDS_API_BASE_URL: str = "https://api.the-odds-api.com/v4"
    ODDS_API_SPORT_KEY: str = "soccer_uefa_champions_league"

    # DuckDB
    DUCKDB_PATH: str = "warehouse/warehouse.duckdb"

    # Misc
    PREFECT_LOGGING_LEVEL: str = "INFO"


settings = Settings()
