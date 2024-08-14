# Built-in imports
import os
from typing import Optional

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

    def upload_object(self, s3_key: str, local_upload_path: str) -> None:
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
                f"upload_object operation failed for: "
                f"bucket_name: {self.s3_bucket_name}. "
                f"s3_key: {s3_key}. "
                f"exc: {exc}."
            )
            raise exc
