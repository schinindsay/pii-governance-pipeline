"""Dagster resources for the data platform."""

from contextlib import AbstractContextManager
from pathlib import Path

import dagster as dg
import duckdb

from data_platform.platform.warehouse.duckdb import connect_warehouse


class DuckDBResource(dg.ConfigurableResource):
    """Connect to the warehouse configured by Dagster definitions."""

    database_path: str

    def connect(self) -> AbstractContextManager[duckdb.DuckDBPyConnection]:
        return connect_warehouse(Path(self.database_path))
