# src/conduit_core/connectors/s3.py

import logging
import os
import tempfile
from pathlib import Path
from typing import Iterable, Dict, Any
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

from .base import BaseSource, BaseDestination
from ..config import Source as SourceConfig
from ..config import Destination as DestinationConfig
from ..utils.retry import retry_with_backoff
from ..errors import ConnectionError

logger = logging.getLogger(__name__)


def _get_s3_client():
    """Helper function to create a boto3 S3 client."""
    load_dotenv()
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    if aws_access_key and aws_secret_key:
        return boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
    return boto3.client('s3', region_name=aws_region)


def _test_s3_connection(s3_client, bucket: str) -> bool:
    """Shared connection test logic for S3 connectors."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except NoCredentialsError:
        raise ConnectionError(
            "AWS credentials not found.\n\n"
            "Suggestions:\n"
            "  • Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your .env file.\n"
            "  • Or configure credentials via `aws configure` for IAM roles."
        ) from None
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            raise ConnectionError(
                f"S3 bucket not found: {bucket}\n\n"
                f"Suggestions:\n"
                f"  • Check the bucket name for typos.\n"
                f"  • Verify the bucket exists in the correct AWS region."
            ) from e
        elif error_code == '403':
            raise ConnectionError(
                f"Access denied to S3 bucket: {bucket}\n\n"
                f"Suggestions:\n"
                f"  • Check your AWS credentials are correct.\n"
                f"  • Verify the IAM user/role has s3:ListBucket permissions on this bucket."
            ) from e
        else:
            raise ConnectionError(f"S3 connection failed with error: {error_code}") from e


class S3Source(BaseSource):
    """Leser data fra Amazon S3 bucket."""

    def __init__(self, config: SourceConfig):
        if not getattr(config, 'bucket', None):
            raise ValueError("S3Source requires a 'bucket' parameter.")
        if not config.path:
            raise ValueError("S3Source requires a 'path' parameter (S3 key).")
        
        self.bucket = config.bucket
        self.key = config.path
        self.s3_client = _get_s3_client()
        logger.info(f"S3Source initialized: s3://{self.bucket}/{self.key}")

    def test_connection(self) -> bool:
        """Test S3 connection and bucket access."""
        return _test_s3_connection(self.s3_client, self.bucket)

    @retry_with_backoff(exceptions=(ClientError,))
    def _download_file(self, temp_path: str):
        """Download file from S3 with retry logic."""
        self.s3_client.download_file(self.bucket, self.key, temp_path)
        logger.info(f"Downloaded s3://{self.bucket}/{self.key} to {temp_path}")

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Leser fil fra S3 og yielder records."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(self.key).suffix) as temp_file:
            temp_path = temp_file.name
        
        try:
            self._download_file(temp_path)
            file_extension = Path(self.key).suffix.lower()
            
            if file_extension == '.csv':
                yield from self._read_csv(temp_path)
            elif file_extension == '.json':
                yield from self._read_json(temp_path)
            else:
                raise ValueError(f"Unsupported file type for S3Source: {file_extension}")
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def _read_csv(self, filepath: str) -> Iterable[Dict[str, Any]]:
        import csv
        with open(filepath, 'r', encoding='utf-8') as f:
            yield from csv.DictReader(f)
    
    def _read_json(self, filepath: str) -> Iterable[Dict[str, Any]]:
        import json
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                yield from data
            else:
                yield data


class S3Destination(BaseDestination):
    """Skriver data til Amazon S3 bucket med atomic uploads."""

    def __init__(self, config: DestinationConfig):
        if not getattr(config, 'bucket', None):
            raise ValueError("S3Destination requires a 'bucket' parameter.")
        if not config.path:
            raise ValueError("S3Destination requires a 'path' parameter (S3 key).")
        
        self.bucket = config.bucket
        self.key = config.path
        self.accumulated_records = []
        self.s3_client = _get_s3_client()
        logger.info(f"S3Destination initialized: s3://{self.bucket}/{self.key}")

    def test_connection(self) -> bool:
        """Test S3 connection and bucket access."""
        return _test_s3_connection(self.s3_client, self.bucket)

    def write(self, records: Iterable[Dict[str, Any]]):
        """Akkumulerer records."""
        self.accumulated_records.extend(list(records))

    @retry_with_backoff(exceptions=(ClientError,))
    def _upload_file(self, temp_path: str):
        """Upload file to S3 with retry logic."""
        self.s3_client.upload_file(temp_path, self.bucket, self.key)
        logger.info(f"✅ Successfully uploaded to s3://{self.bucket}/{self.key}")

    def finalize(self):
        """Skriver alle akkumulerte records til S3."""
        if not self.accumulated_records:
            logger.info("No records to write to S3")
            return

        file_extension = Path(self.key).suffix.lower()
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=file_extension, encoding='utf-8') as temp_file:
            temp_path = temp_file.name

        try:
            if file_extension == '.csv':
                self._write_csv(temp_path, self.accumulated_records)
            elif file_extension == '.json':
                self._write_json(temp_path, self.accumulated_records)
            else:
                raise ValueError(f"Unsupported file type for S3Destination: {file_extension}")
            
            logger.info(f"Writing {len(self.accumulated_records)} records to S3")
            self._upload_file(temp_path)
        finally:
            self.accumulated_records.clear()
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def _write_csv(self, filepath: str, records: list):
        import csv
        if not records: return
        headers = records[0].keys()
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(records)
    
    def _write_json(self, filepath: str, records: list):
        import json
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)