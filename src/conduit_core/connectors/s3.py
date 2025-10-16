# src/conduit_core/connectors/s3.py

import logging
import os
import tempfile
from pathlib import Path
from typing import Iterable, Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

from .base import BaseSource, BaseDestination
from ..config import Source as SourceConfig
from ..config import Destination as DestinationConfig
from ..utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class S3Source(BaseSource):
    """Leser data fra Amazon S3 bucket."""

    def __init__(self, config: SourceConfig):
        load_dotenv()
        
        if not hasattr(config, 'bucket') or not config.bucket:
            raise ValueError("S3Source requires 'bucket' parameter")
        if not config.path:
            raise ValueError("S3Source requires 'path' parameter (S3 key)")
        
        self.bucket = config.bucket
        self.key = config.path
        
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        if aws_access_key and aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
        else:
            self.s3_client = boto3.client('s3', region_name=aws_region)
        
        logger.info(f"S3Source initialized: s3://{self.bucket}/{self.key}")

    @retry_with_backoff(
        max_attempts=3,
        initial_delay=2.0,
        backoff_factor=2.0,
        exceptions=(ClientError, ConnectionError, TimeoutError)
    )
    def _download_file(self, temp_path: str):
        """Download file from S3 with retry logic."""
        self.s3_client.download_file(self.bucket, self.key, temp_path)
        logger.info(f"Downloaded s3://{self.bucket}/{self.key} to {temp_path}")

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Leser fil fra S3 og yielder records."""
        try:
            logger.info(f"Reading from S3: s3://{self.bucket}/{self.key}")
            
            with tempfile.NamedTemporaryFile(mode='w+b', delete=False, suffix=Path(self.key).suffix) as temp_file:
                temp_path = temp_file.name
                
                # Download with retry
                self._download_file(temp_path)
            
            file_extension = Path(self.key).suffix.lower()
            
            if file_extension == '.csv':
                yield from self._read_csv(temp_path)
            elif file_extension == '.json':
                yield from self._read_json(temp_path)
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")
            
            os.unlink(temp_path)
            logger.info(f"Cleaned up temporary file: {temp_path}")
        
        except NoCredentialsError:
            raise ValueError("AWS credentials not found")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            
            if error_code == 'NoSuchBucket':
                raise ValueError(f"S3 bucket '{self.bucket}' does not exist")
            elif error_code == 'NoSuchKey' or error_code == '404':
                raise ValueError(f"S3 key '{self.key}' does not exist")
            else:
                raise ValueError(f"S3 error: {e}")
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e):
                raise ValueError(f"S3 key '{self.key}' does not exist")
            raise ValueError(f"Unexpected error: {e}")
    
    def _read_csv(self, filepath: str) -> Iterable[Dict[str, Any]]:
        import csv
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
    
    def _read_json(self, filepath: str) -> Iterable[Dict[str, Any]]:
        import json
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                for record in data:
                    yield record
            else:
                yield data


class S3Destination(BaseDestination):
    """Skriver data til Amazon S3 bucket med atomic uploads."""

    def __init__(self, config: DestinationConfig):
        load_dotenv()
        
        if not hasattr(config, 'bucket') or not config.bucket:
            raise ValueError("S3Destination requires 'bucket' parameter")
        if not config.path:
            raise ValueError("S3Destination requires 'path' parameter")
        
        self.bucket = config.bucket
        self.key = config.path
        self.accumulated_records = []
        
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        if aws_access_key and aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
        else:
            self.s3_client = boto3.client('s3', region_name=aws_region)
        
        logger.info(f"S3Destination initialized: s3://{self.bucket}/{self.key}")

    def write(self, records: Iterable[Dict[str, Any]]):
        """Akkumulerer records."""
        records_list = list(records)
        logger.info(f"S3Destination.write() accumulating {len(records_list)} records")
        self.accumulated_records.extend(records_list)

    @retry_with_backoff(
        max_attempts=3,
        initial_delay=2.0,
        backoff_factor=2.0,
        exceptions=(ClientError, ConnectionError, TimeoutError)
    )
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
        
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=file_extension, encoding='utf-8') as temp_file:
                temp_path = temp_file.name
                
                if file_extension == '.csv':
                    self._write_csv(temp_path, self.accumulated_records)
                elif file_extension == '.json':
                    self._write_json(temp_path, self.accumulated_records)
                else:
                    raise ValueError(f"Unsupported file type: {file_extension}")
            
            logger.info(f"Writing {len(self.accumulated_records)} records to S3")
            
            # Upload with retry
            self._upload_file(temp_path)
            
            os.unlink(temp_path)
        
        except NoCredentialsError:
            raise ValueError("AWS credentials not found")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                raise ValueError(f"S3 bucket '{self.bucket}' does not exist")
            else:
                raise ValueError(f"S3 error: {e}")
    
    def _write_csv(self, filepath: str, records: list):
        import csv
        headers = records[0].keys()
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(records)
    
    def _write_json(self, filepath: str, records: list):
        import json
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)