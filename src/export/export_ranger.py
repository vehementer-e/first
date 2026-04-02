from __future__ import annotations
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

from src.config.settings import AppSettings
from src.export.cloudian_handler import CloudianHandler
from src.utils.logging import DualLogger

logger = DualLogger(name=__name__)

class RangerExporter:
    """Fetches JSON from a Ranger API endpoint and uploads it to S3."""
    def __init__(self, settings: AppSettings) -> None:
        # Ranger API configuration from settings
        self.api_endpoint = settings.ranger.api_endpoint        # e.g. "https://ranger.company.com/service/public/v2/api/...”
        self.username     = settings.ranger.username
        self.password     = settings.ranger.password.get_secret_value()
        # S3 upload configuration from settings
        self.s3_bucket    = settings.s3.starburst_export_bucket   # or use a new bucket/config for Ranger
        self.s3_export_path = settings.s3.export_default_path
        self.s3_ranger_file = settings.s3.ranger_policies_file
        self.s3_endpoint  = settings.s3.endpoint
        self.s3_access_key= settings.s3.access_key
        self.s3_secret_key= settings.s3.secret_key.get_secret_value()
        self.s3_ssl_verify= settings.s3.ssl_verify

    def fetch_json(self) -> bytes:
        """
        Perform an HTTP GET to the Ranger API endpoint and return raw JSON bytes.
        Uses a retry strategy on failure (e.g. transient 500/502/503/504 errors)
        """
        logger.info(f"Fetching Ranger JSON data from {self.api_endpoint}")
        session = requests.Session()
        retry_strategy = Retry(
            total=3, 
            backoff_factor=0.5, 
            status_forcelist=[500, 502, 503, 504], 
            allowed_methods=["GET"]
        )
        session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        session.mount("http://", HTTPAdapter(max_retries=retry_strategy))

        response = session.get(
            self.api_endpoint,
            auth=(self.username, self.password),
            timeout=(5, 15)  # (connect timeout, read timeout) in seconds
        )
        response.raise_for_status()
        return response.content  # raw JSON bytes

    def export_to_s3(self, data: bytes) -> None:
        """
        Upload raw JSON bytes to S3 (using CloudianHandler).
        """
        s3_path = f"{self.s3_export_path}/{self.s3_ranger_file}"
        logger.info(f"Uploading Ranger JSON to bucket '{self.s3_bucket}' at '{s3_path}'")
        cloudian = CloudianHandler(
            str(self.s3_endpoint),
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_ssl_verify,
        )
        cloudian.upload_file(
            s3_file_path=s3_path,
            file_content=data,
            bucket_name=self.s3_bucket,
        )
        logger.info("Ranger JSON export completed successfully.")
