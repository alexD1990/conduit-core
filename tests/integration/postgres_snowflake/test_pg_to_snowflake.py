"""
Comprehensive Postgres → Snowflake integration tests.
"""
import pytest
import os
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


@pytest.fixture(scope="module")
def postgres_connection():
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='postgres',
        user='postgres',
        password='mysecretpassword'
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def snowflake_connection():
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


def get_snowflake_config(table_name, mode='full_refresh'):
    """Helper to create Snowflake config with proper mode."""
    config = Destination(
        name='snowflake_dest',
        type='snowflake',
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        table=table_name,
        mode=mode
    )
    return config


@pytest.fixture
def cleanup_table(snowflake_connection):
    """Cleanup function that takes table name."""
    tables_to_clean = []
    
    def _cleanup(table_name):
        tables_to_clean.append(table_name)
    
    yield _cleanup
    
    # Cleanup after test
    for table in tables_to_clean:
        try:
            with snowflake_connection.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        except Exception as e:
            print(f"Cleanup warning for {table}: {e}")


class TestBasicIngestion:
    
    def test_simple_transfer_1000_rows(self, postgres_connection, snowflake_connection, cleanup_table):
        """Transfer 1000 rows with basic data types."""
        table_name = 'test_simple'
        cleanup_table(table_name)
        
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_simple")
            cur.execute("""
                CREATE TABLE test_simple (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100),
                    value NUMERIC(10,2),
                    created_at TIMESTAMP
                )
            """)
            cur.execute("""
                INSERT INTO test_simple (id, name, value, created_at)
                SELECT 
                    generate_series,
                    'User_' || generate_series,
                    (random() * 1000)::numeric(10,2),
                    NOW()
                FROM generate_series(1, 1000)
            """)
        postgres_connection.commit()
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost',
                port=5432,
                database='postgres',
                user='postgres',
                password='mysecretpassword'
            )],
            destinations=[get_snowflake_config(table_name)],
            resources=[Resource(
                name='simple_transfer',
                source='pg_source',
                destination='snowflake_dest',
                query='SELECT * FROM test_simple ORDER BY id',
                mode='full_refresh'
            )]
        )
        
        run_resource(config.resources[0], config, skip_preflight=True)
        
        with snowflake_connection.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            result = cur.fetchone()
            assert result[0] == 1000, f"Expected 1000 rows, got {result[0]}"
        
        print(f"\n✓ Successfully transferred 1000 rows to Snowflake")
    
    def test_parallel_extraction_10k_rows(self, postgres_connection, snowflake_connection, cleanup_table):
        """Parallel extraction with 10K rows."""
        table_name = 'test_parallel'
        cleanup_table(table_name)
        
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_parallel")
            cur.execute("""
                CREATE TABLE test_parallel (
                    id INTEGER PRIMARY KEY,
                    data VARCHAR(100)
                )
            """)
            cur.execute("""
                INSERT INTO test_parallel (id, data)
                SELECT generate_series, 'Data_' || generate_series
                FROM generate_series(1, 10000)
            """)
        postgres_connection.commit()
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost',
                port=5432,
                database='postgres',
                user='postgres',
                password='mysecretpassword'
            )],
            destinations=[get_snowflake_config(table_name)],
            resources=[Resource(
                name='parallel_transfer',
                source='pg_source',
                destination='snowflake_dest',
                query='SELECT * FROM test_parallel ORDER BY id',
                mode='full_refresh'
            )],
            parallel_extraction={
                'max_workers': 4,
                'batch_size': 2500
            }
        )
        
        run_resource(config.resources[0], config, skip_preflight=True)
        
        with snowflake_connection.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            total = cur.fetchone()[0]
            
            cur.execute(f'SELECT COUNT(DISTINCT id) FROM "{table_name}"')
            distinct = cur.fetchone()[0]
            
            assert total == 10000, f"Expected 10000 rows, got {total}"
            assert distinct == 10000, f"Found duplicates: {total - distinct}"
        
        print(f"\n✓ Parallel extraction: 10K rows, no duplicates")


class TestDataTypes:
    
    def test_all_postgres_data_types(self, postgres_connection, snowflake_connection, cleanup_table):
        """All major Postgres data types."""
        table_name = 'test_datatypes'
        cleanup_table(table_name)
        
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_datatypes")
            cur.execute("""
                CREATE TABLE test_datatypes (
                    id INTEGER PRIMARY KEY,
                    col_int INTEGER,
                    col_bigint BIGINT,
                    col_numeric NUMERIC(10,2),
                    col_float FLOAT,
                    col_varchar VARCHAR(100),
                    col_text TEXT,
                    col_boolean BOOLEAN,
                    col_date DATE,
                    col_timestamp TIMESTAMP,
                    col_null VARCHAR(50)
                )
            """)
            cur.execute("""
                INSERT INTO test_datatypes VALUES
                (1, 42, 9223372036854775807, 999.99, 3.14159, 'varchar_test', 
                 'text_test', true, '2025-01-01', '2025-01-01 12:30:45', NULL),
                (2, -100, -1000000, -50.25, -2.71828, 'special!@#', 
                 'multiline\ntext', false, '2024-12-31', '2024-12-31 23:59:59', NULL)
            """)
        postgres_connection.commit()
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost',
                port=5432,
                database='postgres',
                user='postgres',
                password='mysecretpassword'
            )],
            destinations=[get_snowflake_config(table_name)],
            resources=[Resource(
                name='datatype_transfer',
                source='pg_source',
                destination='snowflake_dest',
                query='SELECT * FROM test_datatypes ORDER BY id',
                mode='full_refresh'
            )]
        )
        
        run_resource(config.resources[0], config, skip_preflight=True)
        
        with snowflake_connection.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
            rows = cur.fetchall()
            
            assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
            
            row1 = rows[0]
            # Snowflake returns all values as strings from CSV load
            assert int(row1[1]) == 42, f"col_int mismatch"
            assert float(row1[3]) == 999.99, f"col_numeric mismatch"
            assert row1[6] == 'text_test', f"col_text mismatch"
            assert row1[10] is None, f"col_null should be None"
        
        print(f"\n✓ All data types transferred correctly")


class TestModes:
    
    def test_full_refresh_mode(self, postgres_connection, snowflake_connection, cleanup_table):
        """Test full_refresh mode truncates and reloads."""
        table_name = 'test_refresh'
        cleanup_table(table_name)
        
        with postgres_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_refresh")
            cur.execute("""
                CREATE TABLE test_refresh (
                    id INTEGER PRIMARY KEY,
                    value INTEGER
                )
            """)
            cur.execute("INSERT INTO test_refresh VALUES (1, 100), (2, 200)")
        postgres_connection.commit()
        
        config = IngestConfig(
            sources=[Source(
                name='pg_source',
                type='postgres',
                host='localhost',
                port=5432,
                database='postgres',
                user='postgres',
                password='mysecretpassword'
            )],
            destinations=[get_snowflake_config(table_name, mode='full_refresh')],
            resources=[Resource(
                name='refresh_test',
                source='pg_source',
                destination='snowflake_dest',
                query='SELECT * FROM test_refresh',
                mode='full_refresh'
            )]
        )
        
        # First load
        print("\n=== First load ===")
        run_resource(config.resources[0], config, skip_preflight=True)
        
        with snowflake_connection.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            count1 = cur.fetchone()[0]
            print(f"After first load: {count1} rows")
            assert count1 == 2
        
        # Change source data
        with postgres_connection.cursor() as cur:
            cur.execute("DELETE FROM test_refresh WHERE id = 1")
            cur.execute("INSERT INTO test_refresh VALUES (3, 300)")
        postgres_connection.commit()
        
        # Second load (should truncate and reload)
        print("\n=== Second load (full_refresh should truncate) ===")
        run_resource(config.resources[0], config, skip_preflight=True)
        
        with snowflake_connection.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            count2 = cur.fetchone()[0]
            print(f"After second load: {count2} rows")
            
            cur.execute(f'SELECT id, value FROM "{table_name}" ORDER BY id')
            all_rows = cur.fetchall()
            print(f"All rows: {all_rows}")
            
            assert count2 == 2, f"full_refresh should result in 2 rows, got {count2}"
            
            # Snowflake returns strings, convert to int for comparison
            ids = [int(row[0]) for row in all_rows]
            assert ids == [2, 3], f"Expected IDs [2, 3], got {ids}"
        
        print(f"\n✓ full_refresh mode working correctly")
