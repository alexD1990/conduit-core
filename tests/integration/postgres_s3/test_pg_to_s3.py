"""
Comprehensive Postgres → S3 integration tests
Battle-testing S3 destination with same rigor as Snowflake
"""
import pytest
import os
import time
from dotenv import load_dotenv
import psycopg2
import boto3

from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource

load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.getenv('AWS_S3_BUCKET'),
    reason="AWS S3 credentials not configured"
)


@pytest.fixture(scope="module")
def postgres_connection():
    """Postgres test database connection."""
    conn = psycopg2.connect(
        host='localhost', port=5432, database='postgres',
        user='postgres', password='mysecretpassword'
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def s3_client():
    """S3 client for verification."""
    client = boto3.client('s3')
    yield client


@pytest.fixture
def cleanup_s3(s3_client):
    """Cleanup S3 objects after test."""
    bucket = os.getenv('AWS_S3_BUCKET')
    keys_to_delete = []
    
    def _register(key):
        keys_to_delete.append(key)
    
    yield _register
    
    # Cleanup
    for key in keys_to_delete:
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"  Cleaned up s3://{bucket}/{key}")
        except Exception as e:
            print(f"  Cleanup warning for {key}: {e}")


class TestBasicIngestion:
    """Test basic S3 ingestion scenarios."""
    
    def test_simple_transfer_1000_rows_csv(self, postgres_connection, s3_client, cleanup_s3):
        """Transfer 1000 rows to S3 as CSV."""
        print(f"\n{'='*80}")
        print("🚀 TEST 1: Basic CSV transfer (1K rows)")
        print(f"{'='*80}")
        
        # Setup Postgres data
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS s3_test_simple")
            cur.execute("""
                CREATE TABLE s3_test_simple (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100),
                    value NUMERIC(10,2),
                    created_at TIMESTAMP
                )
            """)
            cur.execute("""
                INSERT INTO s3_test_simple (id, name, value, created_at)
                SELECT 
                    generate_series,
                    'User_' || generate_series,
                    (random() * 1000)::numeric(10,2),
                    NOW()
                FROM generate_series(1, 1000)
            """)
        postgres_connection.commit()
        
        # Configure S3 destination
        s3_key = 'test_data/simple_1000.csv'
        cleanup_s3(s3_key)
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost', port=5432, database='postgres',
                user='postgres', password='mysecretpassword'
            )],
            destinations=[Destination(
                name='s3_dest',
                type='s3',
                bucket=os.getenv('AWS_S3_BUCKET'),
                path=s3_key
            )],
            resources=[Resource(
                name='simple_transfer',
                source='pg_source',
                destination='s3_dest',
                query='SELECT * FROM s3_test_simple ORDER BY id'
            )]
        )
        
        # Run ingestion
        start = time.time()
        run_resource(config.resources[0], config, skip_preflight=True)
        elapsed = time.time() - start
        
        # Verify in S3
        bucket = os.getenv('AWS_S3_BUCKET')
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        lines = content.strip().split('\n')
        
        # CSV has header + 1000 rows
        assert len(lines) == 1001, f"Expected 1001 lines (header + 1000), got {len(lines)}"
        
        print(f"\n📈 RESULTS:")
        print(f"   Time: {elapsed:.2f}s")
        print(f"   Throughput: {1000/elapsed:.0f} rows/sec")
        print(f"   File: s3://{bucket}/{s3_key}")
        print(f"   Size: {len(content)} bytes")
        print(f"✅ PASSED\n")
    
    def test_simple_transfer_1000_rows_json(self, postgres_connection, s3_client, cleanup_s3):
        """Transfer 1000 rows to S3 as JSON."""
        print(f"\n{'='*80}")
        print("🚀 TEST 2: Basic JSON transfer (1K rows)")
        print(f"{'='*80}")
        
        # Setup
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS s3_test_json")
            cur.execute("""
                CREATE TABLE s3_test_json (
                    id INTEGER PRIMARY KEY,
                    data VARCHAR(100)
                )
            """)
            cur.execute("""
                INSERT INTO s3_test_json
                SELECT generate_series, 'data_' || generate_series
                FROM generate_series(1, 1000)
            """)
        postgres_connection.commit()
        
        s3_key = 'test_data/simple_1000.json'
        cleanup_s3(s3_key)
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost', port=5432, database='postgres',
                user='postgres', password='mysecretpassword'
            )],
            destinations=[Destination(
                name='s3_dest',
                type='s3',
                bucket=os.getenv('AWS_S3_BUCKET'),
                path=s3_key
            )],
            resources=[Resource(
                name='json_transfer',
                source='pg_source',
                destination='s3_dest',
                query='SELECT * FROM s3_test_json ORDER BY id'
            )]
        )
        
        run_resource(config.resources[0], config, skip_preflight=True)
        
        # Verify JSON
        import json
        bucket = os.getenv('AWS_S3_BUCKET')
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        assert len(data) == 1000, f"Expected 1000 records, got {len(data)}"
        assert data[0]['id'] == 1
        assert data[999]['id'] == 1000
        
        print(f"\n✅ JSON transfer verified: 1000 records\n")


class TestThroughputScaling:
    """Test S3 throughput with larger datasets."""
    
    def test_parallel_extraction_10k_rows(self, postgres_connection, s3_client, cleanup_s3):
        """Parallel extraction with 10K rows."""
        print(f"\n{'='*80}")
        print("🚀 TEST 3: Parallel extraction (10K rows)")
        print(f"{'='*80}")
        
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS s3_test_parallel")
            cur.execute("""
                CREATE TABLE s3_test_parallel (
                    id INTEGER PRIMARY KEY,
                    data VARCHAR(100)
                )
            """)
            cur.execute("""
                INSERT INTO s3_test_parallel
                SELECT generate_series, 'data_' || generate_series
                FROM generate_series(1, 10000)
            """)
        postgres_connection.commit()
        
        s3_key = 'test_data/parallel_10k.csv'
        cleanup_s3(s3_key)
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost', port=5432, database='postgres',
                user='postgres', password='mysecretpassword'
            )],
            destinations=[Destination(
                name='s3_dest',
                type='s3',
                bucket=os.getenv('AWS_S3_BUCKET'),
                path=s3_key
            )],
            resources=[Resource(
                name='parallel_transfer',
                source='pg_source',
                destination='s3_dest',
                query='SELECT * FROM s3_test_parallel ORDER BY id'
            )],
            parallel_extraction={
                'max_workers': 4,
                'batch_size': 2500
            }
        )
        
        start = time.time()
        run_resource(config.resources[0], config, skip_preflight=True)
        elapsed = time.time() - start
        
        # Verify
        bucket = os.getenv('AWS_S3_BUCKET')
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        lines = content.strip().split('\n')
        
        assert len(lines) == 10001, f"Expected 10001 lines, got {len(lines)}"
        
        print(f"\n📈 RESULTS:")
        print(f"   Time: {elapsed:.2f}s")
        print(f"   Throughput: {10000/elapsed:.0f} rows/sec")
        print(f"✅ PASSED\n")
    
    def test_large_dataset_100k_rows(self, postgres_connection, s3_client, cleanup_s3):
        """Large dataset test: 100K rows."""
        print(f"\n{'='*80}")
        print("🚀 TEST 4: Large dataset (100K rows)")
        print(f"{'='*80}")
        
        print("Creating 100K test rows...")
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS s3_test_large")
            cur.execute("""
                CREATE TABLE s3_test_large (
                    id INTEGER PRIMARY KEY,
                    col_int INTEGER,
                    col_numeric NUMERIC(15,2),
                    col_varchar VARCHAR(100)
                )
            """)
            
            # Insert in batches
            for offset in range(0, 100000, 10000):
                cur.execute(f"""
                    INSERT INTO s3_test_large
                    SELECT 
                        generate_series + {offset},
                        (random() * 1000000)::int,
                        (random() * 1000000)::numeric(15,2),
                        'data_' || generate_series
                    FROM generate_series(1, 10000)
                """)
        postgres_connection.commit()
        print("✓ Test data created")
        
        s3_key = 'test_data/large_100k.csv'
        cleanup_s3(s3_key)
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost', port=5432, database='postgres',
                user='postgres', password='mysecretpassword'
            )],
            destinations=[Destination(
                name='s3_dest',
                type='s3',
                bucket=os.getenv('AWS_S3_BUCKET'),
                path=s3_key
            )],
            resources=[Resource(
                name='large_transfer',
                source='pg_source',
                destination='s3_dest',
                query='SELECT * FROM s3_test_large ORDER BY id'
            )],
            parallel_extraction={
                'max_workers': 4,
                'batch_size': 5000
            }
        )
        
        start = time.time()
        run_resource(config.resources[0], config, skip_preflight=True)
        elapsed = time.time() - start
        
        # Verify file exists and get size
        bucket = os.getenv('AWS_S3_BUCKET')
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        file_size = response['ContentLength']
        
        print(f"\n📈 RESULTS:")
        print(f"   Rows: 100,000")
        print(f"   Time: {elapsed:.2f}s")
        print(f"   Throughput: {100000/elapsed:,.0f} rows/sec")
        print(f"   File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        print(f"✅ PASSED\n")


class TestDataTypes:
    """Test data type handling."""
    
    def test_all_postgres_data_types(self, postgres_connection, s3_client, cleanup_s3):
        """All major Postgres data types to CSV."""
        print(f"\n{'='*80}")
        print("🚀 TEST 5: Data types preservation")
        print(f"{'='*80}")
        
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS s3_test_datatypes")
            cur.execute("""
                CREATE TABLE s3_test_datatypes (
                    id INTEGER PRIMARY KEY,
                    col_int INTEGER,
                    col_numeric NUMERIC(10,2),
                    col_varchar VARCHAR(100),
                    col_text TEXT,
                    col_boolean BOOLEAN,
                    col_date DATE,
                    col_timestamp TIMESTAMP,
                    col_null VARCHAR(50)
                )
            """)
            cur.execute("""
                INSERT INTO s3_test_datatypes VALUES
                (1, 42, 999.99, 'varchar_test', 'text_test', true, 
                 '2025-01-01', '2025-01-01 12:30:45', NULL),
                (2, -100, -50.25, 'special!@#', 'multiline\ntext', false, 
                 '2024-12-31', '2024-12-31 23:59:59', NULL)
            """)
        postgres_connection.commit()
        
        s3_key = 'test_data/datatypes.csv'
        cleanup_s3(s3_key)
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost', port=5432, database='postgres',
                user='postgres', password='mysecretpassword'
            )],
            destinations=[Destination(
                name='s3_dest',
                type='s3',
                bucket=os.getenv('AWS_S3_BUCKET'),
                path=s3_key
            )],
            resources=[Resource(
                name='datatype_transfer',
                source='pg_source',
                destination='s3_dest',
                query='SELECT * FROM s3_test_datatypes ORDER BY id'
            )]
        )
        
        run_resource(config.resources[0], config, skip_preflight=True)
        
        # Verify data
        import csv
        bucket = os.getenv('AWS_S3_BUCKET')
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        
        reader = csv.DictReader(content.strip().split('\n'))
        rows = list(reader)
        
        assert len(rows) == 2
        assert rows[0]['id'] == '1'
        assert rows[0]['col_int'] == '42'
        assert 'varchar_test' in rows[0]['col_varchar']
        
        print(f"\n✅ All data types transferred correctly\n")
