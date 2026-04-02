"""Utilities for connecting to Trino and fetching column metadata."""

from contextlib import closing

import pandas as pd
from trino.auth import BasicAuthentication
from trino.dbapi import connect

from src.config.settings import AppSettings
from src.utils.logging import DualLogger

logger = DualLogger(name=__name__)

TRINO_QUERY = """
    SELECT
        table_cat   AS table_catalog,
        table_schem AS table_schema,
        table_name,
        column_name,
        ordinal_position,
        column_def  AS column_default,
        nullable    AS is_nullable,
        remarks
    FROM system.jdbc.columns
    WHERE table_cat = 'delta'
"""


class TrinoMetadataExtractor:  # pylint: disable=too-few-public-methods
    """Connects to Starburst/Trino and fetches column metadata."""

    def __init__(self, settings: AppSettings) -> None:
        """Initialize extractor from application settings."""
        self.settings = settings
        self.host = self.settings.trino.host
        self.port = self.settings.trino.port
        self.user = self.settings.trino.user
        self.password = self.settings.trino.password.get_secret_value()
        self.catalogs = self.settings.trino.catalogs

    def fetch_columns(self) -> pd.DataFrame:
        """
        Fetch column metadata from Trino.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing column metadata with the same columns
            as returned by `system.jdbc.columns`.
        """
        logger.info(f"Connecting to {self.host}:{self.port} as {self.user}")
        with closing(
            connect(
                host=self.host,
                port=self.port,
                user=self.user,
                http_scheme="https",
                auth=BasicAuthentication(self.user, self.password),
                verify=True,
            )
        ) as conn, closing(conn.cursor()) as cur:
            cur.execute(TRINO_QUERY)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

        logger.info(f"Fetch columns OK — {df.shape[0]} rows, {df.shape[1]} columns")

        return df
