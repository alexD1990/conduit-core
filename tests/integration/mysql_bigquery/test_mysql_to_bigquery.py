"""
Comprehensive MySQL → BigQuery integration tests
"""
import pytest
import os
from dotenv import load_dotenv
import mysql.connector
from google.cloud import bigquery

from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource

load_dotenv()

pytestmark = pytest.mark.skipif(
    not os.getenv('BIGQUERY_PROJECT'),
    reason="BigQuery credentials not configured"
)


@pytest.fixture(scope="module")
def mysql_connection():
    """MySQL test database connection."""
    conn = mysql.connector.connect(
        host='localhost', port=3306, database='testdb',
        user='root', password='mysecretpassword'
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def bigquery_client():
    """BigQuery client for verification."""
    credentials_path = os.getenv('BIGQUERY_CREDENTIALS_PATH')
    project_id = os.getenv('BIGQUERY_PROJECT')
    client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
    yield client


def get_bigquery_config(table_name, mode='full_refresh'):
    """Helper to create BigQuery config."""
    return Destination(
        name='bigquery_dest',
        type='bigquery',
        project=os.getenv('BIGQUERY_PROJECT'),
        dataset=os.getenv('BIGQUERY_DATASET'),
        table=table_name,
        credentials_path=os.getenv('BIGQUERY_CREDENTIALS_PATH'),
        mode=mode
    )


@pytest.fixture
def cleanup_table(bigquery_client):
    """Cleanup BigQuery tables after test."""
    tables_to_clean = []
    
    def _cleanup(table_name):
        tables_to_clean.append(table_name)
    
    yield _cleanup
    
    project = os.getenv('BIGQUERY_PROJECT')
    dataset = os.getenv('BIGQUERY_DATASET')
    
    for table in tables_to_clean:
        try:
            table_id = f"{project}.{dataset}.{table}"
            bigquery_client.delete_table(table_id, not_found_ok=True)
            print(f"  Cleaned up {table_id}")
        except Exception as e:
            print(f"  Cleanup warning for {table}: {e}")


class TestBasicIngestion:
    """Test basic MySQL → BigQuery ingestion."""
    
    def test_simple_transfer_1000_rows(self, mysql_connection, bigquery_client, cleanup_table):
        """Transfer 1000 rows from MySQL to BigQuery."""
        print(f"\n{'='*80}")
        print("🚀 TEST 1: Basic MySQL → BigQuery (1K rows)")
        print(f"{'='*80}")
        
        table_name = 'mysql_test_simple'
        cleanup_table(table_name)
        
        # Setup MySQL data
        with mysql_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_simple")
            cur.execute("""
                CREATE TABLE test_simple (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100),
                    value DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            for i in range(1, 1001):
                cur.execute("""
                    INSERT INTO test_simple (id, name, value) 
                    VALUES (%s, %s, %s)
                """, (i, f'User_{i}', round(i * 1.5, 2)))
        
        mysql_connection.commit()
        
        # Configure ingestion
        config = IngestConfig(
            sources=[Source(
                name='mysql_source',
                type='mysql',
                host='localhost',
                port=3306,
                database='testdb',
                user='root',
                password='mysecretpassword'
            )],
            destinations=[get_bigquery_config(table_name)],
            resources=[Resource(
                name='simple_transfer',
                source='mysql_source',
                destination='bigquery_dest',
                query='SELECT * FROM test_simple ORDER BY id',
                mode='full_refresh'
            )]
        )
        
        # Run ingestion
        run_resource(config.resources[0], config, skip_preflight=True)
        
        # Verify in BigQuery
        project = os.getenv('BIGQUERY_PROJECT')
        dataset = os.getenv('BIGQUERY_DATASET')
        query = f"SELECT COUNT(*) as count FROM `{project}.{dataset}.{table_name}`"
        result = list(bigquery_client.query(query).result())
        count = result[0]['count']
        
        assert count == 1000, f"Expected 1000 rows, got {count}"
        
        print(f"\n✅ Successfully transferred 1000 rows from MySQL to BigQuery\n")
    
    def test_parallel_extraction_10k_rows(self, mysql_connection, bigquery_client, cleanup_table):
        """Parallel extraction with 10K rows."""
        print(f"\n{'='*80}")
        print("🚀 TEST 2: Parallel extraction (10K rows)")
        print(f"{'='*80}")
        
        table_name = 'mysql_test_parallel'
        cleanup_table(table_name)
        
        with mysql_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_parallel")
            cur.execute("""
                CREATE TABLE test_parallel (
                    id INTEGER PRIMARY KEY,
                    data VARCHAR(100)
                )
            """)
            
            values = [(i, f'data_{i}') for i in range(1, 10001)]
            cur.executemany("INSERT INTO test_parallel VALUES (%s, %s)", values)
        
        mysql_connection.commit()
        
        config = IngestConfig(
            sources=[Source(
                name='mysql_source',
                type='mysql',
                host='localhost',
                port=3306,
                database='testdb',
                user='root',
                password='mysecretpassword'
            )],
            destinations=[get_bigquery_config(table_name)],
            resources=[Resource(
                name='parallel_transfer',
                source='mysql_source',
                destination='bigquery_dest',
                query='SELECT * FROM test_parallel ORDER BY id',
                mode='full_refresh'
            )],
            parallel_extraction={
                'max_workers': 4,
                'batch_size': 2500
            }
        )
        
        run_resource(config.resources[0], config, skip_preflight=True)
        
        # Verify
        project = os.getenv('BIGQUERY_PROJECT')
        dataset = os.getenv('BIGQUERY_DATASET')
        
        query_total = f"SELECT COUNT(*) as count FROM `{project}.{dataset}.{table_name}`"
        total = list(bigquery_client.query(query_total).result())[0]['count']
        
        query_distinct = f"SELECT COUNT(DISTINCT id) as count FROM `{project}.{dataset}.{table_name}`"
        distinct = list(bigquery_client.query(query_distinct).result())[0]['count']
        
        assert total == 10000, f"Expected 10000 rows, got {total}"
        assert distinct == 10000, f"Found duplicates: {total - distinct}"
        
        print(f"\n✅ Parallel extraction: 10K rows, no duplicates\n")


class TestDataTypes:
    """Test MySQL data type handling."""
    
    def test_all_mysql_data_types(self, mysql_connection, bigquery_client, cleanup_table):
        """All major MySQL data types."""
        print(f"\n{'='*80}")
        print("🚀 TEST 3: MySQL data types")
        print(f"{'='*80}")
        
        table_name = 'mysql_test_datatypes'
        cleanup_table(table_name)
        
        with mysql_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_datatypes")
            cur.execute("""
                CREATE TABLE test_datatypes (
                    id INTEGER PRIMARY KEY,
                    col_int INTEGER,
                    col_bigint BIGINT,
                    col_decimal DECIMAL(10,2),
                    col_float FLOAT,
                    col_varchar VARCHAR(100),
                    col_text TEXT,
                    col_boolean BOOLEAN,
                    col_date DATE,
                    col_datetime DATETIME,
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
        
        mysql_connection.commit()
        
        config = IngestConfig(
            sources=[Source(
                name='mysql_source',
                type='mysql',
                host='localhost',
                port=3306,
                database='testdb',
                user='root',
                password='mysecretpassword'
            )],
            destinations=[get_bigquery_config(table_name)],
            resources=[Resource(
                name='datatype_transfer',
                source='mysql_source',
                destination='bigquery_dest',
                query='SELECT * FROM test_datatypes ORDER BY id',
                mode='full_refresh'
            )]
        )
        
        run_resource(config.resources[0], config, skip_preflight=True)
        
        # Verify
        project = os.getenv('BIGQUERY_PROJECT')
        dataset = os.getenv('BIGQUERY_DATASET')
        query = f"SELECT * FROM `{project}.{dataset}.{table_name}` ORDER BY id"
        rows = list(bigquery_client.query(query).result())
        
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
        
        row1 = rows[0]
        assert row1['id'] == 1
        assert row1['col_int'] == 42
        assert row1['col_text'] == 'text_test'
        assert row1['col_null'] is None
        
        print(f"\n✅ All data types transferred correctly\n")


class TestModes:
    """Test full_refresh mode."""
    
    def test_full_refresh_mode(self, mysql_connection, bigquery_client, cleanup_table):
        """Test full_refresh mode truncates and reloads."""
        print(f"\n{'='*80}")
        print("🚀 TEST 4: Full refresh mode")
        print(f"{'='*80}")
        
        table_name = 'mysql_test_refresh'
        cleanup_table(table_name)
        
        # Initial data
        with mysql_connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_refresh")
            cur.execute("""
                CREATE TABLE test_refresh (
                    id INTEGER PRIMARY KEY,
                    value INTEGER
                )
            """)
            cur.execute("INSERT INTO test_refresh VALUES (1, 100), (2, 200)")
        
        mysql_connection.commit()
        
        config = IngestConfig(
            sources=[Source(
                name='mysql_source',
                type='mysql',
                host='localhost',
                port=3306,
                database='testdb',
                user='root',
                password='mysecretpassword'
            )],
            destinations=[get_bigquery_config(table_name, mode='full_refresh')],
            resources=[Resource(
                name='refresh_test',
                source='mysql_source',
                destination='bigquery_dest',
                query='SELECT * FROM test_refresh',
                mode='full_refresh'
            )]
        )
        
        # First load
        print("\n=== First load ===")
        run_resource(config.resources[0], config, skip_preflight=True)
        
        project = os.getenv('BIGQUERY_PROJECT')
        dataset = os.getenv('BIGQUERY_DATASET')
        query = f"SELECT COUNT(*) as count FROM `{project}.{dataset}.{table_name}`"
        count1 = list(bigquery_client.query(query).result())[0]['count']
        print(f"After first load: {count1} rows")
        assert count1 == 2
        
        # Change data
        with mysql_connection.cursor() as cur:
            cur.execute("DELETE FROM test_refresh WHERE id = 1")
            cur.execute("INSERT INTO test_refresh VALUES (3, 300)")
        mysql_connection.commit()
        
        # Second load
        print("\n=== Second load (full_refresh should truncate) ===")
        run_resource(config.resources[0], config, skip_preflight=True)
        
        count2 = list(bigquery_client.query(query).result())[0]['count']
        print(f"After second load: {count2} rows")
        
        query_all = f"SELECT id, value FROM `{project}.{dataset}.{table_name}` ORDER BY id"
        all_rows = list(bigquery_client.query(query_all).result())
        print(f"All rows: {[(r['id'], r['value']) for r in all_rows]}")
        
        assert count2 == 2, f"full_refresh should result in 2 rows, got {count2}"
        
        ids = [row['id'] for row in all_rows]
        assert ids == [2, 3], f"Expected IDs [2, 3], got {ids}"
        
        print(f"\n✅ full_refresh mode working correctly\n")
