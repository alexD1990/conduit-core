# tests/test_s3_connector.py

import pytest
import os
from moto import mock_aws
import boto3
from conduit_core.connectors.s3 import S3Source, S3Destination
from conduit_core.config import Source as SourceConfig
from conduit_core.config import Destination as DestinationConfig


@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for testing."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_REGION'] = 'us-east-1'


@pytest.fixture
def s3_client(aws_credentials):
    """Create a mocked S3 client."""
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        yield s3


@pytest.fixture
def test_bucket(s3_client):
    """Create a test S3 bucket."""
    bucket_name = 'test-bucket'
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name


def test_s3_source_reads_csv(s3_client, test_bucket):
    """Test that S3Source can read a CSV file from S3."""
    # Upload a test CSV to S3
    csv_content = "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com"
    s3_client.put_object(
        Bucket=test_bucket,
        Key='data/test.csv',
        Body=csv_content.encode('utf-8')
    )
    
    # Create S3Source
    config = SourceConfig(
        name='test_source',
        type='s3',
        bucket=test_bucket,
        path='data/test.csv'
    )
    source = S3Source(config)
    
    # Read records
    records = list(source.read())
    
    # Verify
    assert len(records) == 2
    assert records[0]['id'] == '1'
    assert records[0]['name'] == 'Alice'
    assert records[1]['name'] == 'Bob'


def test_s3_source_reads_json(s3_client, test_bucket):
    """Test that S3Source can read a JSON file from S3."""
    # Upload a test JSON to S3
    json_content = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
    s3_client.put_object(
        Bucket=test_bucket,
        Key='data/test.json',
        Body=json_content.encode('utf-8')
    )
    
    # Create S3Source
    config = SourceConfig(
        name='test_source',
        type='s3',
        bucket=test_bucket,
        path='data/test.json'
    )
    source = S3Source(config)
    
    # Read records
    records = list(source.read())
    
    # Verify
    assert len(records) == 2
    assert records[0]['id'] == 1
    assert records[0]['name'] == 'Alice'


def test_s3_destination_writes_csv(s3_client, test_bucket):
    """Test that S3Destination can write a CSV file to S3."""
    # Create S3Destination
    config = DestinationConfig(
        name='test_dest',
        type='s3',
        bucket=test_bucket,
        path='output/result.csv'
    )
    destination = S3Destination(config)
    
    # Write records
    records = [
        {'id': '1', 'name': 'Alice', 'email': 'alice@example.com'},
        {'id': '2', 'name': 'Bob', 'email': 'bob@example.com'}
    ]
    destination.write(records)
    
    # Verify file exists in S3
    response = s3_client.get_object(Bucket=test_bucket, Key='output/result.csv')
    content = response['Body'].read().decode('utf-8')
    
    assert 'id,name,email' in content
    assert 'Alice' in content
    assert 'Bob' in content


def test_s3_destination_writes_json(s3_client, test_bucket):
    """Test that S3Destination can write a JSON file to S3."""
    # Create S3Destination
    config = DestinationConfig(
        name='test_dest',
        type='s3',
        bucket=test_bucket,
        path='output/result.json'
    )
    destination = S3Destination(config)
    
    # Write records
    records = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]
    destination.write(records)
    
    # Verify file exists in S3
    response = s3_client.get_object(Bucket=test_bucket, Key='output/result.json')
    content = response['Body'].read().decode('utf-8')
    
    assert 'Alice' in content
    assert 'Bob' in content


def test_s3_source_missing_bucket_raises_error():
    """Test that S3Source raises error when bucket is missing."""
    config = SourceConfig(
        name='test_source',
        type='s3',
        path='data/test.csv'
        # bucket is missing
    )
    
    with pytest.raises(ValueError, match="requires 'bucket'"):
        S3Source(config)


def test_s3_source_nonexistent_bucket_raises_error(s3_client):
    """Test that S3Source raises error when bucket doesn't exist."""
    config = SourceConfig(
        name='test_source',
        type='s3',
        bucket='nonexistent-bucket',
        path='data/test.csv'
    )
    source = S3Source(config)
    
    with pytest.raises(ValueError, match="does not exist"):
        list(source.read())


def test_s3_source_nonexistent_key_raises_error(s3_client, test_bucket):
    """Test that S3Source raises error when key doesn't exist."""
    config = SourceConfig(
        name='test_source',
        type='s3',
        bucket=test_bucket,
        path='data/nonexistent.csv'
    )
    source = S3Source(config)
    
    with pytest.raises(ValueError, match="does not exist"):
        list(source.read())


def test_s3_destination_empty_records(s3_client, test_bucket):
    """Test that S3Destination handles empty records gracefully."""
    config = DestinationConfig(
        name='test_dest',
        type='s3',
        bucket=test_bucket,
        path='output/empty.csv'
    )
    destination = S3Destination(config)
    
    # Write empty list - should not create file
    destination.write([])
    
    # Verify no file was created
    objects = s3_client.list_objects_v2(Bucket=test_bucket, Prefix='output/empty.csv')
    assert 'Contents' not in objects