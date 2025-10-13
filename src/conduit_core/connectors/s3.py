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

logger = logging.getLogger(__name__)


class S3Source(BaseSource):
    """Leser data fra Amazon S3 bucket."""

    def __init__(self, config: SourceConfig):
        load_dotenv()
        
        # Required fields
        if not hasattr(config, 'bucket') or not config.bucket:
            raise ValueError("S3Source requires 'bucket' parameter")
        if not config.path:
            raise ValueError("S3Source requires 'path' parameter (S3 key)")
        
        self.bucket = config.bucket
        self.key = config.path
        
        # Optional AWS credentials (falls back to default AWS config)
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        # Initialize S3 client
        if aws_access_key and aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
        else:
            # Use default AWS credentials (from ~/.aws/credentials or IAM role)
            self.s3_client = boto3.client('s3', region_name=aws_region)
        
        logger.info(f"S3Source initialized: s3://{self.bucket}/{self.key}")

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """
        Leser fil fra S3 og yielder records.
        
        For CSV/JSON filer, parser og yielder rader.
        Støtter kun enkeltfiler for nå (ikke wildcards).
        """
        try:
            logger.info(f"Reading from S3: s3://{self.bucket}/{self.key}")
            
            # Download file to temporary location
            with tempfile.NamedTemporaryFile(mode='w+b', delete=False, suffix=Path(self.key).suffix) as temp_file:
                temp_path = temp_file.name
                
                # Download from S3
                self.s3_client.download_file(self.bucket, self.key, temp_path)
                logger.info(f"Downloaded s3://{self.bucket}/{self.key} to {temp_path}")
            
            # Determine file type and parse accordingly
            file_extension = Path(self.key).suffix.lower()
            
            if file_extension == '.csv':
                yield from self._read_csv(temp_path)
            elif file_extension == '.json':
                yield from self._read_json(temp_path)
            else:
                raise ValueError(f"Unsupported file type: {file_extension}. Supported: .csv, .json")
            
            # Cleanup temp file
            os.unlink(temp_path)
            logger.info(f"Cleaned up temporary file: {temp_path}")
        
        except NoCredentialsError:
            raise ValueError("AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env or configure ~/.aws/credentials")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            if error_code == 'NoSuchBucket':
                raise ValueError(f"S3 bucket '{self.bucket}' does not exist")
            elif error_code == 'NoSuchKey' or error_code == '404':
                raise ValueError(f"S3 key '{self.key}' does not exist in bucket '{self.bucket}'")
            else:
                raise ValueError(f"S3 error ({error_code}): {error_message}")
        except Exception as e:
            # Catch any other errors (like moto 404)
            if "404" in str(e) or "Not Found" in str(e):
                raise ValueError(f"S3 key '{self.key}' does not exist in bucket '{self.bucket}'")
            raise ValueError(f"Unexpected error reading from S3: {e}")
    
    def _read_csv(self, filepath: str) -> Iterable[Dict[str, Any]]:
        """Read CSV file and yield records."""
        import csv
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
    
    def _read_json(self, filepath: str) -> Iterable[Dict[str, Any]]:
        """Read JSON file and yield records."""
        import json
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Handle both array of objects and single object
            if isinstance(data, list):
                for record in data:
                    yield record
            else:
                yield data


class S3Destination(BaseDestination):
    """Skriver data til Amazon S3 bucket med atomic uploads."""

    def __init__(self, config: DestinationConfig):
        load_dotenv()
        
        # Required fields
        if not hasattr(config, 'bucket') or not config.bucket:
            raise ValueError("S3Destination requires 'bucket' parameter")
        if not config.path:
            raise ValueError("S3Destination requires 'path' parameter (S3 key)")
        
        self.bucket = config.bucket
        self.key = config.path
        
        # NEW: Akkumuler alle records
        self.accumulated_records = []
        
        # Optional AWS credentials
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
        
        # Initialize S3 client
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
        """
        Akkumulerer records. Actual upload skjer i finalize().
        """
        records_list = list(records)
        logger.info(f"🔍 S3Destination.write() accumulating {len(records_list)} records")
        self.accumulated_records.extend(records_list)
    
    def finalize(self):
        """
        Upload alle akkumulerte records til S3.
        Kalles automatisk ved slutten av pipeline.
        """
        if not self.accumulated_records:
            logger.info("No records to write to S3")
            return

        # Determine file type from key extension
        file_extension = Path(self.key).suffix.lower()
        
        try:
            # Create temp file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=file_extension, encoding='utf-8') as temp_file:
                temp_path = temp_file.name
                
                # Write data to temp file based on format
                if file_extension == '.csv':
                    self._write_csv(temp_path, self.accumulated_records)
                elif file_extension == '.json':
                    self._write_json(temp_path, self.accumulated_records)
                else:
                    raise ValueError(f"Unsupported file type: {file_extension}. Supported: .csv, .json")
            
            logger.info(f"Writing {len(self.accumulated_records)} records to S3: s3://{self.bucket}/{self.key}")
            
            # Upload to S3 (atomic operation)
            self.s3_client.upload_file(temp_path, self.bucket, self.key)
            
            logger.info(f"✅ Successfully uploaded to s3://{self.bucket}/{self.key}")
            
            # Cleanup temp file
            os.unlink(temp_path)
        
        except NoCredentialsError:
            raise ValueError("AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                raise ValueError(f"S3 bucket '{self.bucket}' does not exist")
            else:
                raise ValueError(f"S3 error: {e}")
    
    def _write_csv(self, filepath: str, records: list):
        """Write records to CSV file."""
        import csv
        headers = records[0].keys()
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(records)
    
    def _write_json(self, filepath: str, records: list):
        """Write records to JSON file."""
        import json
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)