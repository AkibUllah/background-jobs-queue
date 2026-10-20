"""Environment-driven configuration for the CLI and dashboard."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="JOBQ_", env_file=".env", extra="ignore")

    redis_url: str | None = Field(default=None, description="redis:// URL; None = memory backend")
    prefix: str = "jobq:"
    app: str | None = Field(default=None, description="dotted path module:attribute to a JobQ")
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8000
    cors_origins: str = "http://localhost:5173"
    queues: str = "default"
    concurrency: int = 10
    log_level: str = "INFO"

    @property
    def queue_list(self) -> list[str]:
        return [q.strip() for q in self.queues.split(",") if q.strip()]

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]
