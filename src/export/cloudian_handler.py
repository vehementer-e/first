"""Provides CloudianHandler for managing S3-compatible file uploads and downloads
with logging and error handling."""

from pathlib import Path
from typing import List
import traceback
import boto3

from src.utils.logging import DualLogger

logger = DualLogger(name=__name__)


class CloudianHandler:
    """Handles Cloudian (S3-compatible) file uploads and downloads."""
    def __init__(self,
                 endpoint_url: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 ssl_verify: str
    ):
        """Initializes the Cloudian handler and S3 client based on configuration."""

        self._s3_client = boto3.client(
            "s3",
            endpoint_url = endpoint_url,
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            verify = False #ssl_verify
        )


    @property
    def client(self):
        """
        Backwards-compatible accessor so that other classes expecting
        .client or .s3_client still work without modification.
        """
        return self._s3_client

    @property
    def s3_client(self):
        """
        Preferred accessor inside this class.
        """
        return self._s3_client

    def upload_file(self, s3_file_path: str,
                    file_content: str,
                    bucket_name: str) -> str:
        """Uploads a file to Cloudian and returns the S3 key."""
        try:
            resp = self._s3_client.put_object(
                Bucket = bucket_name,
                Key = s3_file_path,
                Body = file_content)
            logger.info(resp)
            logger.info(f"Uploaded {len(file_content)} bytes to s3://{bucket_name}/{s3_file_path}")

            return s3_file_path

        except Exception as e:
            logger.error(f"Failed to upload to s3://{bucket_name}/"
                         f"{s3_file_path} - {str(e)} \n "
                         f"traceback.format_exc = {traceback.format_exc()}",
                         event_id="CIA_STARBURST_cloudian_handler_001",
                         event_action="Escalate - Check Cloudian",
                         event_flag=True)
            raise

    def download_files(self, bucket_name: str, s3_directory: str, output_path: Path) -> List[str]:
        """Downloads all files from a specified S3 directory to a local folder."""
        downloaded_files = []
        try:
            paginator = self._s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_directory)

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    relative_path = Path(key).relative_to(s3_directory)
                    local_file_path = output_path / relative_path

                    local_file_path.parent.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Downloading file {key} to {local_file_path}")
                    self._s3_client.download_file(bucket_name, key, str(local_file_path))
                    downloaded_files.append(str(local_file_path))

            if downloaded_files:
                logger.info(
                    f"Successfully downloaded {len(downloaded_files)} file(s) from s3://"
                    f"{bucket_name}/{s3_directory}"
                )
            else:
                logger.warning(f"No files found in s3://{bucket_name}/{s3_directory}")

            return downloaded_files

        except Exception as e:
            logger.error(
                f"Failed to download files from Cloudian: {str(e)} \n "
                f"traceback.format_exc = {traceback.format_exc()}",
                event_id="CIA_SGD_cloudian_handler_002",
                event_action="Escalate - Check Cloudian",
                event_flag=True)

            raise
