from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    pg_dsn: str = "postgresql://postgres:postgres@localhost:5432/faers"
    data_root: str = "data/faers"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()