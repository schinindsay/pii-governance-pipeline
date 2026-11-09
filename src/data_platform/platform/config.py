"""Environment-specific platform configuration."""

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
VALID_ENVIRONMENTS = {"local", "dev", "staging", "prod"}


@dataclass(frozen=True)
class PlatformSettings:
    environment: str
    warehouse_path: Path


def load_settings() -> PlatformSettings:
    """Load and validate the runtime environment."""
    environment = os.getenv("DATA_PLATFORM_ENV", "local").lower()

    if environment not in VALID_ENVIRONMENTS:
        raise ValueError(f"Unsupported environment: {environment}")

    configured_path = os.getenv("DATA_PLATFORM_WAREHOUSE_PATH")

    if configured_path:
        warehouse_path = Path(configured_path).expanduser().resolve()
    elif environment == "local":
        warehouse_path = PROJECT_ROOT / "data" / "warehouse.duckdb"
    else:
        raise ValueError(
            f"DATA_PLATFORM_WAREHOUSE_PATH is required for environment {environment!r}"
        )

    return PlatformSettings(
        environment=environment,
        warehouse_path=warehouse_path,
    )
