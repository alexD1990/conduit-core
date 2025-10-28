"""
S3 Throughput Stress Test
Validates S3 destination performance at scale
"""
import pytest
import os
import time
import psutil
from dotenv import load_dotenv
import psycopg2
import boto3

from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource

load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.getenv('AWS_S3_BUCKET'),
    reason="AWS credentials not configured"
)


class TestS3Throughput:
    """Test S3 throughput with different configurations."""
    
    @pytest.fixture(scope="class")
    def postgres_conn(self):
        conn = psycopg2.connect(
            host='localhost', port=5432, database='postgres',
            user='postgres', password='mysecretpassword'
        )
        yield conn
        conn.close()
    
    @pytest.fixture(scope="class")
    def s3_client(self):
        client = boto3.client('s3')
        yield client
    
    @pytest.fixture(autouse=True)
    def cleanup(self, s3_client):
        yield
        bucket = os.getenv('AWS_S3_BUCKET')
        keys = ['stress/throughput_100k.csv', 'stress/throughput_500k.csv']
        for key in keys:
            try:
                s3_client.delete_object(Bucket=bucket, Key=key)
            except:
                pass
    
    def create_test_data(self, postgres_conn, row_count):
        """Create test dataset."""
        print(f"\n📊 Creating {row_count:,} test rows...")
        start = time.time()
        
        with postgres_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS s3_stress_source")
            cur.execute("""
                CREATE TABLE s3_stress_source (
                    id BIGINT PRIMARY KEY,
                    col_int INTEGER,
                    col_numeric NUMERIC(15,2),
                    col_varchar VARCHAR(100),
                    col_timestamp TIMESTAMP
                )
            """)
            
            batch_size = 10000
            for offset in range(0, row_count, batch_size):
                limit = min(batch_size, row_count - offset)
                cur.execute(f"""
                    INSERT INTO s3_stress_source
                    SELECT 
                        generate_series + {offset},
                        (random() * 1000000)::int,
                        (random() * 1000000)::numeric(15,2),
                        'data_' || generate_series,
                        CURRENT_TIMESTAMP - (random() * 365 * 86400)::int * interval '1 second'
                    FROM generate_series(1, {limit})
                """)
        
        postgres_conn.commit()
        elapsed = time.time() - start
        print(f"✓ Test data created in {elapsed:.1f}s")
    
    def run_test(self, workers, batch_size, row_count, s3_key):
        """Run ingestion test."""
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
                name='throughput_test',
                source='pg_source',
                destination='s3_dest',
                query='SELECT * FROM s3_stress_source ORDER BY id'
            )],
            parallel_extraction={
                'max_workers': workers,
                'batch_size': batch_size
            } if workers > 1 else None
        )
        
        process = psutil.Process()
        start_mem = process.memory_info().rss / 1024 / 1024
        start_time = time.time()
        
        run_resource(config.resources[0], config, skip_preflight=True)
        
        elapsed = time.time() - start_time
        end_mem = process.memory_info().rss / 1024 / 1024
        
        return {
            'elapsed': round(elapsed, 2),
            'throughput': round(row_count / elapsed, 1),
            'memory_delta': round(end_mem - start_mem, 1)
        }
    
    def test_baseline_100k(self, postgres_conn, s3_client):
        """Baseline: 100K rows."""
        print(f"\n{'='*80}")
        print("🚀 S3 TEST 1: Baseline (100K rows)")
        print(f"{'='*80}")
        
        self.create_test_data(postgres_conn, 100_000)
        metrics = self.run_test(
            workers=4, 
            batch_size=5000, 
            row_count=100_000,
            s3_key='stress/throughput_100k.csv'
        )
        
        bucket = os.getenv('AWS_S3_BUCKET')
        response = s3_client.head_object(Bucket=bucket, Key='stress/throughput_100k.csv')
        file_size = response['ContentLength']
        
        print(f"\n�� RESULTS:")
        print(f"   Time: {metrics['elapsed']}s")
        print(f"   Throughput: {metrics['throughput']:,.0f} rows/sec")
        print(f"   Memory Δ: {metrics['memory_delta']} MB")
        print(f"   File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        
        assert metrics['throughput'] > 10000, "Too slow"
        print("✅ PASSED\n")
    
    def test_scale_500k(self, postgres_conn, s3_client):
        """Scale test: 500K rows."""
        print(f"\n{'='*80}")
        print("🚀 S3 TEST 2: Scale (500K rows)")
        print(f"{'='*80}")
        
        self.create_test_data(postgres_conn, 500_000)
        metrics = self.run_test(
            workers=4,
            batch_size=10000,
            row_count=500_000,
            s3_key='stress/throughput_500k.csv'
        )
        
        bucket = os.getenv('AWS_S3_BUCKET')
        response = s3_client.head_object(Bucket=bucket, Key='stress/throughput_500k.csv')
        file_size = response['ContentLength']
        
        print(f"\n📈 RESULTS:")
        print(f"   Time: {metrics['elapsed']}s")
        print(f"   Throughput: {metrics['throughput']:,.0f} rows/sec")
        print(f"   Memory Δ: {metrics['memory_delta']} MB")
        print(f"   File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        
        assert metrics['throughput'] > 10000
        print("✅ PASSED\n")
