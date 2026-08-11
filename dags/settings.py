import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = "/app/config.yaml"


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load the non-secret application configuration."""
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError(f"Configuration in {path} must be a YAML mapping")
    return config


def require_credential(name: str) -> str:
    """Read one credential from the process environment."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required credential: {name}")
    return value


def minio_url(config: dict[str, Any]) -> str:
    minio_config = config["minio"]
    scheme = "https" if minio_config["secure"] else "http"
    return f"{scheme}://{minio_config['endpoint']}"
