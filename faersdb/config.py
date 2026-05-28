import re
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_MEMORY_LIMIT_RE = re.compile(r"^\d+(\.\d+)?\s*[KMGT]?B$", re.IGNORECASE)


def sql_string(value: str) -> str:
    """Return a single-quoted DuckDB SQL string literal, safe against injection."""
    return "'" + value.replace("'", "''") + "'"


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

    @field_validator("memory_limit")
    @classmethod
    def validate_memory_limit(cls, value: str) -> str:
        cleaned = value.strip()
        if not _MEMORY_LIMIT_RE.match(cleaned):
            raise ValueError(
                f"Invalid memory_limit '{value}'. "
                "Expected a value like '2GB', '512MB', or '1.5GB'."
            )
        return cleaned

    @property
    def warehouse_path(self) -> Path:
        return Path(self.warehouse_dir)


settings = Settings()