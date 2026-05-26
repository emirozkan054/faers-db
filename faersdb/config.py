from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    data_root: str = "data/faers"
    warehouse_dir: str = "warehouse"
    memory_limit: str = "2GB"
    threads: int = 4

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        extra="ignore",
    )

    @property
    def warehouse_path(self) -> Path:
        return Path(self.warehouse_dir)


settings = Settings()