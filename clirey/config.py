"""Configuration and Broker URL resolution for Clirey."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlunparse

from pydantic import BaseModel, Field


def mask_broker_url(url: str) -> str:
    """Mask password and sensitive credentials in broker URL for display."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        if parsed.password:
            netloc = f"{parsed.username or ''}:****@{parsed.hostname or ''}"
            if parsed.port:
                netloc += f":{parsed.port}"
            masked = parsed._replace(netloc=netloc)
            return urlunparse(masked)
        return url
    except Exception:
        # Fallback regex mask
        return re.sub(r":([^@]+)@", r":****@", url)


class ClireyConfig(BaseModel):
    """Clirey configuration settings."""

    broker_url: str = Field(default="redis://localhost:6379/0", description="Celery broker connection URL")
    result_backend: Optional[str] = Field(default=None, description="Optional Celery result backend URL")
    refresh_rate: float = Field(default=1.5, description="Dashboard refresh rate in seconds")
    timeout: float = Field(default=2.5, description="Timeout for inspect / ping requests")
    event_buffer_size: int = Field(default=1000, description="Max event log buffer size")
    max_history_tasks: int = Field(default=500, description="Max task history records")

    @classmethod
    def resolve_broker_url(cls, explicit_url: Optional[str] = None) -> str:
        """Resolve broker URL by checking:
        1. Explicitly passed parameter
        2. Environment variables: CELERY_BROKER_URL, CLIREY_BROKER_URL, REDIS_URL, RABBITMQ_URL, AMQP_URL
        3. Config file in current directory or user home
        4. Default localhost redis
        """
        if explicit_url and explicit_url.strip():
            return explicit_url.strip()

        env_keys = [
            "CLIREY_BROKER_URL",
            "CELERY_BROKER_URL",
            "REDIS_URL",
            "RABBITMQ_URL",
            "AMQP_URL",
        ]
        for key in env_keys:
            val = os.environ.get(key)
            if val and val.strip():
                return val.strip()

        # Check for config files (.clirey.json or ~/.clirey.json)
        local_cfg = Path(".clirey.json")
        home_cfg = Path.home() / ".clirey.json"
        for p in [local_cfg, home_cfg]:
            if p.exists() and p.is_file():
                try:
                    import json

                    data = json.loads(p.read_text(encoding="utf-8"))
                    if "broker_url" in data and data["broker_url"]:
                        return str(data["broker_url"]).strip()
                except Exception:
                    pass

        return "redis://localhost:6379/0"
