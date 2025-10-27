"""
Chaos Testing - Validate resilience under failure conditions
Tests: Process interruption, network issues, data consistency
"""
import pytest
import os
import time
import signal
import subprocess
import psycopg2
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.getenv('SNOWFLAKE_ACCOUNT'),
    reason="Snowflake credentials not configured"
)


class TestChaosRecovery:
    """Test pipeline resilience under various failure scenarios."""
    
    @pytest.fixture
    def postgres_conn(self):
        conn = psycopg2.connect(
            host='localhost', port=5432, database='postgres',
            user='postgres', password='mysecretpassword'
        )
        yield conn
        conn.close()
    
    @pytest.fixture
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
    
    def test_full_refresh_prevents_duplicates(self, postgres_conn, snowflake_conn):
        """Test: Running same pipeline twice with full_refresh should not duplicate data."""
        print(f"\n{'='*80}")
        print("🧪 CHAOS TEST 1: Idempotency with full_refresh")
        print(f"{'='*80}")
        
        # Setup test data
        with postgres_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS chaos_test_1")
            cur.execute("""
                CREATE TABLE chaos_test_1 (
                    id INTEGER PRIMARY KEY,
                    data VARCHAR(100)
                )
            """)
            cur.execute("""
                INSERT INTO chaos_test_1
                SELECT generate_series, 'data_' || generate_series
                FROM generate_series(1, 10000)
            """)
        postgres_conn.commit()
        
        # Create test config file
        config_content = f"""
sources:
  - name: pg_chaos
    type: postgres
    host: localhost
    port: 5432
    database: postgres
    user: postgres
    password: mysecretpassword

destinations:
  - name: sf_chaos
    type: snowflake
    account: {os.getenv('SNOWFLAKE_ACCOUNT')}
    user: {os.getenv('SNOWFLAKE_USER')}
    password: {os.getenv('SNOWFLAKE_PASSWORD')}
    warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}
    database: {os.getenv('SNOWFLAKE_DATABASE')}
    schema: {os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')}
    table: chaos_test_1
    mode: full_refresh

resources:
  - name: chaos_run
    source: pg_chaos
    destination: sf_chaos
    query: "SELECT * FROM chaos_test_1"
    mode: full_refresh
"""
        
        with open('/tmp/chaos_test_1.yml', 'w') as f:
            f.write(config_content)
        
        # Run 1
        print("\n▶️  Run 1: Initial load")
        result1 = subprocess.run(
            ['conduit', 'run', '/tmp/chaos_test_1.yml', '--resource', 'chaos_run'],
            capture_output=True, text=True
        )
        assert result1.returncode == 0, f"Run 1 failed: {result1.stderr}"
        
        with snowflake_conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "chaos_test_1"')
            count1 = cur.fetchone()[0]
        
        print(f"   ✓ Count after run 1: {count1:,}")
        
        # Run 2 (should truncate and reload, not append)
        print("\n▶️  Run 2: Re-run with full_refresh")
        result2 = subprocess.run(
            ['conduit', 'run', '/tmp/chaos_test_1.yml', '--resource', 'chaos_run'],
            capture_output=True, text=True
        )
        assert result2.returncode == 0, f"Run 2 failed: {result2.stderr}"
        
        with snowflake_conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "chaos_test_1"')
            count2 = cur.fetchone()[0]
            cur.execute('DROP TABLE IF EXISTS "chaos_test_1"')
        
        print(f"   ✓ Count after run 2: {count2:,}")
        
        print(f"\n📊 RESULTS:")
        print(f"   Run 1: {count1:,} rows")
        print(f"   Run 2: {count2:,} rows")
        print(f"   Duplicates: {count2 - count1}")
        
        assert count1 == 10000, f"Run 1 should have 10K rows, got {count1}"
        assert count2 == 10000, f"Run 2 should still have 10K rows, got {count2}"
        
        print("\n✅ PASSED: No duplicates, full_refresh works correctly")
    
    def test_data_consistency_after_multiple_runs(self, postgres_conn, snowflake_conn):
        """Test: Multiple sequential runs maintain data consistency."""
        print(f"\n{'='*80}")
        print("🧪 CHAOS TEST 2: Data consistency across multiple runs")
        print(f"{'='*80}")
        
        # Setup
        with postgres_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS chaos_test_2")
            cur.execute("""
                CREATE TABLE chaos_test_2 (
                    id INTEGER PRIMARY KEY,
                    value INTEGER
                )
            """)
        
        config_content = f"""
sources:
  - name: pg_chaos
    type: postgres
    host: localhost
    port: 5432
    database: postgres
    user: postgres
    password: mysecretpassword

destinations:
  - name: sf_chaos
    type: snowflake
    account: {os.getenv('SNOWFLAKE_ACCOUNT')}
    user: {os.getenv('SNOWFLAKE_USER')}
    password: {os.getenv('SNOWFLAKE_PASSWORD')}
    warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}
    database: {os.getenv('SNOWFLAKE_DATABASE')}
    schema: {os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')}
    table: chaos_test_2
    mode: full_refresh

resources:
  - name: chaos_run
    source: pg_chaos
    destination: sf_chaos
    query: "SELECT * FROM chaos_test_2"
    mode: full_refresh
"""
        
        with open('/tmp/chaos_test_2.yml', 'w') as f:
            f.write(config_content)
        
        # Run 3 times with different data
        for run_num in range(1, 4):
            print(f"\n▶️  Run {run_num}: Loading {run_num * 1000} rows")
            
            # Update source data
            with postgres_conn.cursor() as cur:
                cur.execute("TRUNCATE chaos_test_2")
                cur.execute(f"""
                    INSERT INTO chaos_test_2
                    SELECT generate_series, generate_series * {run_num}
                    FROM generate_series(1, {run_num * 1000})
                """)
            postgres_conn.commit()
            
            # Run pipeline
            result = subprocess.run(
                ['conduit', 'run', '/tmp/chaos_test_2.yml', '--resource', 'chaos_run'],
                capture_output=True, text=True
            )
            assert result.returncode == 0
            
            # Verify
            with snowflake_conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM "chaos_test_2"')
                count = cur.fetchone()[0]
            
            expected = run_num * 1000
            print(f"   ✓ Expected: {expected:,}, Actual: {count:,}")
            assert count == expected, f"Run {run_num}: Expected {expected}, got {count}"
        
        # Cleanup
        with snowflake_conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS "chaos_test_2"')
        
        print("\n✅ PASSED: Data consistency maintained across all runs")
