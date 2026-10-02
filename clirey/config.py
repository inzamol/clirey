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
    global_keyprefix: Optional[str] = Field(default=None, description="Redis global key prefix")
    key_prefix: Optional[str] = Field(default=None, description="Alias for global_keyprefix")
    broker_transport_options: Optional[dict] = Field(default=None, description="Broker transport options")
    refresh_rate: float = Field(default=1.5, description="Dashboard refresh rate in seconds")
    timeout: float = Field(default=2.5, description="Timeout for inspect / ping requests")
    event_buffer_size: int = Field(default=1000, description="Max event log buffer size")
    max_history_tasks: int = Field(default=500, description="Max task history records")

    @classmethod
    def _read_config_file(cls) -> dict:
        """Read configuration from candidate JSON config files."""
        config_candidates = [
            Path(".clirey.json"),
            Path(".celirey.json"),
            Path("clirey.json"),
            Path.home() / ".clirey.json",
            Path.home() / ".celirey.json",
        ]
        for p in config_candidates:
            if p.exists() and p.is_file():
                try:
                    import json

                    return json.loads(p.read_text(encoding="utf-8"))
                except Exception:
                    pass
        return {}

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

        file_cfg = cls._read_config_file()
        if "broker_url" in file_cfg and file_cfg["broker_url"]:
            return str(file_cfg["broker_url"]).strip()

        return "redis://localhost:6379/0"

    @classmethod
    def resolve_key_prefix(cls, explicit_prefix: Optional[str] = None) -> Optional[str]:
        """Resolve Redis global_keyprefix / key_prefix by checking:
        1. Explicitly passed parameter
        2. Environment variables: CELERY_GLOBAL_KEYPREFIX, GLOBAL_KEYPREFIX, CLIREY_KEY_PREFIX, CLIREY_GLOBAL_KEYPREFIX, CELERY_REDIS_KEYPREFIX, CELERY_BROKER_TRANSPORT_OPTIONS
        3. Config file: global_keyprefix, key_prefix, or broker_transport_options.global_keyprefix
        4. None (allows auto-detection from Redis)
        """
        if explicit_prefix is not None and explicit_prefix.strip():
            return explicit_prefix.strip()

        env_keys = [
            "CLIREY_KEY_PREFIX",
            "CLIREY_GLOBAL_KEYPREFIX",
            "CELERY_GLOBAL_KEYPREFIX",
            "GLOBAL_KEYPREFIX",
            "CELERY_REDIS_KEYPREFIX",
        ]
        for key in env_keys:
            val = os.environ.get(key)
            if val is not None and val.strip():
                return val.strip()

        # Check CELERY_BROKER_TRANSPORT_OPTIONS env var
        bto_env = os.environ.get("CELERY_BROKER_TRANSPORT_OPTIONS")
        if bto_env and bto_env.strip():
            try:
                import json

                parsed_bto = json.loads(bto_env)
                if isinstance(parsed_bto, dict):
                    prefix = parsed_bto.get("global_keyprefix") or parsed_bto.get("key_prefix")
                    if prefix:
                        return str(prefix).strip()
            except Exception:
                pass

        # Check config files
        file_cfg = cls._read_config_file()
        if file_cfg:
            if "global_keyprefix" in file_cfg and file_cfg["global_keyprefix"]:
                return str(file_cfg["global_keyprefix"]).strip()
            if "key_prefix" in file_cfg and file_cfg["key_prefix"]:
                return str(file_cfg["key_prefix"]).strip()
            if "keyprefix" in file_cfg and file_cfg["keyprefix"]:
                return str(file_cfg["keyprefix"]).strip()
            if "broker_transport_options" in file_cfg and isinstance(file_cfg["broker_transport_options"], dict):
                bto = file_cfg["broker_transport_options"]
                prefix = bto.get("global_keyprefix") or bto.get("key_prefix")
                if prefix:
                    return str(prefix).strip()

        return None
