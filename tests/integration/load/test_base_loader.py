import duckdb
import pytest
from pyspark.sql import SparkSession
from etl.load.base_loader import BaseLoader

# Sample data for testing
sample_data = [
    {"user_id": "u1", "track_id": "t1", "timestamp": 12345},
    {"user_id": "u2", "track_id": "t2", "timestamp": 12346},
]

ddl_statement = """
CREATE TABLE IF NOT EXISTS listens (
    user_id VARCHAR PRIMARY KEY,
    track_id VARCHAR,
    timestamp BIGINT
)
"""

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestLoader").getOrCreate()

@pytest.fixture
def duckdb_conn():
    conn = duckdb.connect(database=':memory:')
    yield conn
    conn.close()

def test_base_loader_creates_table_and_inserts_data(spark, duckdb_conn):
    df = spark.createDataFrame(sample_data)

    loader = BaseLoader(
        db_conn=duckdb_conn,
        ddl=ddl_statement,
        table_name="listens",
        columns=["user_id", "track_id", "timestamp"],
        dedup_columns=["user_id"]
    )

    loader.load_data(df)

    result = duckdb_conn.execute("SELECT * FROM listens").fetchall()
    assert len(result) == 2
    assert result[0] == ("u1", "t1", 12345)
    assert result[1] == ("u2", "t2", 12346)
