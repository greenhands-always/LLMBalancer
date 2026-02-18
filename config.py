"""Global configuration for ULRDS."""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    # Authentication
    auth_token: str = os.environ.get("ULRDS_AUTH_TOKEN", "change-me-in-production")

    # Database
    db_path: str = os.environ.get("ULRDS_DB_PATH", "ulrds.db")

    # Provider config file
    providers_yaml: str = os.environ.get("ULRDS_PROVIDERS_YAML", "providers.yaml")

    # Server
    host: str = os.environ.get("ULRDS_HOST", "0.0.0.0")
    port: int = int(os.environ.get("ULRDS_PORT", "8000"))

    # Logging
    log_level: str = os.environ.get("ULRDS_LOG_LEVEL", "INFO")


settings = Settings()
