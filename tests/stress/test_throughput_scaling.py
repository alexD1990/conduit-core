"""
Throughput Scaling Stress Test
Tests sustained throughput and stability as data size grows
"""
import pytest
import os
import time
import psutil
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import snowflake.connector

from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource

load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.getenv('SNOWFLAKE_ACCOUNT'),
    reason="Snowflake credentials not configured"
)


class TestThroughputScaling:
    """Test throughput with different worker counts and batch sizes."""
    
    @pytest.fixture(scope="class")
    def postgres_conn(self):
        conn = psycopg2.connect(
            host='localhost', port=5432, database='postgres',
            user='postgres', password='mysecretpassword'
        )
        yield conn
        conn.close()
    
    @pytest.fixture(scope="class")
    def snowflake_conn(self):
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        )
        yield conn
        conn.close()
    
    @pytest.fixture(autouse=True)
    def cleanup(self, snowflake_conn):
        yield
        try:
            with snowflake_conn.cursor() as cur:
                cur.execute('DROP TABLE IF EXISTS "stress_test_throughput"')
        except:
            pass
    
    def create_test_data(self, postgres_conn, row_count):
        """Create test dataset in Postgres."""
        print(f"\n📊 Creating {row_count:,} test rows...")
        start = time.time()
        
        with postgres_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS stress_test_source")
            cur.execute("""
                CREATE TABLE stress_test_source (
                    id BIGINT PRIMARY KEY,
                    col_int INTEGER,
                    col_numeric NUMERIC(15,2),
                    col_varchar VARCHAR(100),
                    col_timestamp TIMESTAMP
                )
            """)
            
            # Insert in batches
            batch_size = 10000
            for offset in range(0, row_count, batch_size):
                limit = min(batch_size, row_count - offset)
                cur.execute(f"""
                    INSERT INTO stress_test_source
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
    
    def run_test(self, workers, batch_size, row_count):
        """Run ingestion test and collect metrics."""
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost', port=5432, database='postgres',
                user='postgres', password='mysecretpassword'
            )],
            destinations=[Destination(
                name='sf_dest',
                type='snowflake',
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
                table='stress_test_throughput',
                mode='full_refresh'
            )],
            resources=[Resource(
                name='throughput_test',
                source='pg_source',
                destination='sf_dest',
                query='SELECT * FROM stress_test_source ORDER BY id',
                mode='full_refresh'
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
    
    def test_baseline_1worker_100k(self, postgres_conn, snowflake_conn):
        """Baseline: 1 worker, 100K rows."""
        print(f"\n{'='*80}")
        print("🚀 TEST 1: Baseline (1 worker, 100K rows)")
        print(f"{'='*80}")
        
        self.create_test_data(postgres_conn, 100_000)
        metrics = self.run_test(workers=1, batch_size=1000, row_count=100_000)
        
        with snowflake_conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "stress_test_throughput"')
            count = cur.fetchone()[0]
        
        print(f"\n📈 RESULTS:")
        print(f"   Time: {metrics['elapsed']}s")
        print(f"   Throughput: {metrics['throughput']:,.0f} rows/sec")
        print(f"   Memory Δ: {metrics['memory_delta']} MB")
        print(f"   Verified: {count:,} rows")
        
        assert count == 100_000
        print("✅ PASSED\n")
    
    def test_parallel_2workers_100k(self, postgres_conn, snowflake_conn):
        """2 workers, 100K rows - check scaling."""
        print(f"\n{'='*80}")
        print("🚀 TEST 2: Parallel (2 workers, 100K rows)")
        print(f"{'='*80}")
        
        self.create_test_data(postgres_conn, 100_000)
        metrics = self.run_test(workers=2, batch_size=2500, row_count=100_000)
        
        with snowflake_conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "stress_test_throughput"')
            count = cur.fetchone()[0]
        
        print(f"\n📈 RESULTS:")
        print(f"   Time: {metrics['elapsed']}s")
        print(f"   Throughput: {metrics['throughput']:,.0f} rows/sec")
        print(f"   Memory Δ: {metrics['memory_delta']} MB")
        
        assert count == 100_000
        print("✅ PASSED\n")
    
    def test_parallel_4workers_500k(self, postgres_conn, snowflake_conn):
        """4 workers, 500K rows - scale test."""
        print(f"\n{'='*80}")
        print("🚀 TEST 3: Scale (4 workers, 500K rows)")
        print(f"{'='*80}")
        
        self.create_test_data(postgres_conn, 500_000)
        metrics = self.run_test(workers=4, batch_size=5000, row_count=500_000)
        
        with snowflake_conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "stress_test_throughput"')
            count = cur.fetchone()[0]
        
        print(f"\n📈 RESULTS:")
        print(f"   Time: {metrics['elapsed']}s")
        print(f"   Throughput: {metrics['throughput']:,.0f} rows/sec")
        print(f"   Memory Δ: {metrics['memory_delta']} MB")
        
        assert count == 500_000
        print("✅ PASSED\n")
