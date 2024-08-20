# Built-in imports
import os
import json
from typing import Optional, Any

# External imports
import boto3
from botocore.exceptions import ClientError

# Own imports
from common.logger import custom_logger

logger = custom_logger()


class S3Helper:
    """Custom S3 Helper for simplifying read/write operations to S3."""

    def __init__(self, s3_bucket_name: str) -> None:
        """
        :param s3_bucket_name (str): Name of the S3 Bucket to interact with.
        """
        self.s3_bucket_name = s3_bucket_name
        self.s3_client = boto3.client("s3")

    def download_object(self, s3_key: str, download_path: str) -> Optional[dict]:
        """
        Method to get an object from the S3 bucket and save it locally.
        :param s3_key (str): The key of the object in the S3 bucket.
        :param download_path (str): The local file path where the object will be saved.
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket_name,
                Key=s3_key,
            )

            # Make sure the "/tmp" directory exists
            if not os.path.exists(download_path):
                os.makedirs(os.path.dirname(download_path), exist_ok=True)

            with open(download_path, "wb") as f:
                f.write(response["Body"].read())
            return response

        except ClientError as exc:
            logger.error(
                f"get_object operation failed for: "
                f"bucket_name: {self.s3_bucket_name}. "
                f"s3_key: {s3_key}. "
                f"exc: {exc}."
            )
            raise exc

    def upload_object_from_file(self, s3_key: str, local_upload_path: str) -> None:
        """
        Method to upload an object to the S3 bucket.
        :param s3_key (str): The key of the object in the S3 bucket.
        :param local_upload_path (str): The local file path of the object to upload.
        """
        try:
            with open(local_upload_path, "rb") as f:
                self.s3_client.upload_fileobj(
                    f,
                    self.s3_bucket_name,
                    s3_key,
                )

        except ClientError as exc:
            logger.error(
                f"upload_object_from_file operation failed for: "
                f"bucket_name: {self.s3_bucket_name}. "
                f"s3_key: {s3_key}. "
                f"exc: {exc}."
            )
            raise exc

    def upload_object_from_memory(self, s3_key: str, data: Any) -> None:
        """
        Method to upload a JSON object to an S3 bucket.
        :param s3_key (str): The key of the object in the S3 bucket.
        :param data (Any): The in memory data to upload to S3 (e.g., a dictionary).
        """
        try:
            self.s3_client.put_object(
                Body=json.dumps(data, skipkeys=True, default=str),
                Bucket=self.s3_bucket_name,
                Key=s3_key,
            )

        except ClientError as exc:
            logger.error(
                f"upload_object_from_file operation failed for: "
                f"bucket_name: {self.s3_bucket_name}. "
                f"s3_key: {s3_key}. "
                f"exc: {exc}."
            )
            raise exc

    def upload_binary_object(self, s3_key: str, data: bytes) -> None:
        """
        Method to upload binary data (e.g., a video file) to an S3 bucket.
        :param s3_key (str): The key of the object in the S3 bucket.
        :param data (bytes): The binary data to upload to S3.
        """
        try:
            self.s3_client.put_object(
                Body=data,
                Bucket=self.s3_bucket_name,
                Key=s3_key,
            )

        except ClientError as exc:
            logger.error(
                f"upload_binary_object operation failed for: "
                f"bucket_name: {self.s3_bucket_name}. "
                f"s3_key: {s3_key}. "
                f"exc: {exc}."
            )
            raise exc
