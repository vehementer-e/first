"""Entry point for exporting Starburst metadata from Trino to S3."""

import logging
import traceback
import sys

from src.config.settings import AppSettings
from src.export import TrinoMetadataExtractor
from src.export.export_ranger import RangerExporter
from src.export.export_starburst import ExportStarburst
from src.utils.decorators import timing_decorator
from src.utils.logging import DualLogger

logger = DualLogger(name=__name__)


@timing_decorator
def main() -> None:
    """Extract Starburst metadata from Trino and export it to S3."""
    # Load configuration
    settings = AppSettings.from_env()

    # Configure root logger
    logging.basicConfig(level=settings.logging.level)

    starburst_data_df = TrinoMetadataExtractor(settings) \
                        .fetch_columns()

    ExportStarburst(settings) \
        .export_to_s3(starburst_data_df)

    logger.info("Exported {starburst_data_df.shape[0]} lines")

    ranger_exporter = RangerExporter(settings)
    ranger_json = ranger_exporter.fetch_json()
    ranger_exporter.export_to_s3(ranger_json)
    
    logger.info(f"ranger policies exported: {len(ranger_json)} bytes");


if __name__ == "__main__":
    try:
        main()
    except Exception: # pylint: disable=broad-exception-caught
        traceback.print_exc()
        sys.exit(1)
