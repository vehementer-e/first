"""Export Starburst/Trino metadata to S3-compatible storage."""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd

from src.config.settings import AppSettings
from src.export.cloudian_handler import CloudianHandler
from src.utils.logging import DualLogger

logger = DualLogger(name=__name__)


class ExportStarburst:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """
    Export Starburst column metadata to an S3-compatible object storage.

    Note: if the exported payload becomes too large, this class may need to be
    extended to split the data into multiple objects.
    """

    def __init__(self, settings: AppSettings) -> None:
        """Initialize exporter with application settings."""
        self.output_path = Path(settings.output.directory)
        self.file_prefix = settings.output.file_prefix
        self.file_size = settings.output.file_size

        self.s3_bucket = settings.s3.starburst_export_bucket
        self.s3_export_default_path = settings.s3.export_default_path
        self.s3_endpoint = settings.s3.endpoint
        self.s3_access_key = settings.s3.access_key
        self.s3_secret_key = settings.s3.secret_key
        self.s3_starburst_columns_file = settings.s3.starburst_columns_file
        self.s3_ssl_verify = settings.s3.ssl_verify

    def export_to_s3(self, df: pd.DataFrame) -> None:
        """
        Serialize the given DataFrame as JSON Lines and upload it to S3.

        Parameters
        ----------
        df : pandas.DataFrame
            The dataframe containing Starburst column metadata.
        """
        logger.info(
            f"Exporting dataframe ({df.shape[0]} columns) to bucket '{self.s3_bucket}'"
            f" at '{self.s3_export_default_path}/{self.s3_starburst_columns_file}'"
        )

        buffer = io.StringIO()
        df.to_json(buffer, orient="records", lines=True)
        data = buffer.getvalue().encode("utf-8")

        cloudian = CloudianHandler(
            str(self.s3_endpoint),
            self.s3_access_key,
            self.s3_secret_key.get_secret_value(),
            self.s3_ssl_verify,
        )
        cloudian.upload_file(
            s3_file_path=(
                f"{self.s3_export_default_path}/"
                f"{self.s3_starburst_columns_file}"
            ),
            file_content=data,
            bucket_name=self.s3_bucket,
        )

        logger.info("Export to S3 completed successfully.")
