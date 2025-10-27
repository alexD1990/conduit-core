"""Minimal throughput test - uses <0.01 credits"""
import pytest
import os
import time
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

def test_mini_throughput():
    """Minimal test: 10K rows, 1 worker - verify cost/performance"""
    
    # Setup Postgres
    conn = psycopg2.connect(
        host='localhost', port=5432, database='postgres',
        user='postgres', password='mysecretpassword'
    )
    
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS mini_test")
        cur.execute("""
            CREATE TABLE mini_test (
                id INTEGER PRIMARY KEY,
                data VARCHAR(100)
            )
        """)
        cur.execute("""
            INSERT INTO mini_test
            SELECT generate_series, 'data_' || generate_series
            FROM generate_series(1, 10000)
        """)
    conn.commit()
    
    # Run ingestion
    start = time.time()
    config = IngestConfig(
        sources=[Source(
            name='pg', type='postgres',
            host='localhost', port=5432, database='postgres',
            user='postgres', password='mysecretpassword'
        )],
        destinations=[Destination(
            name='sf', type='snowflake',
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            table='mini_test', mode='full_refresh'
        )],
        resources=[Resource(
            name='mini', source='pg', destination='sf',
            query='SELECT * FROM mini_test', mode='full_refresh'
        )]
    )
    
    run_resource(config.resources[0], config, skip_preflight=True)
    elapsed = time.time() - start
    
    # Verify
    sf_conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    )
    
    with sf_conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM "mini_test"')
        count = cur.fetchone()[0]
        cur.execute('DROP TABLE IF EXISTS "mini_test"')
    
    sf_conn.close()
    conn.close()
    
    print(f"\n✅ Mini test: {count:,} rows in {elapsed:.1f}s = {count/elapsed:.0f} rows/sec")
    print(f"📊 Estimated credits used: <0.001")
    
    assert count == 10000
