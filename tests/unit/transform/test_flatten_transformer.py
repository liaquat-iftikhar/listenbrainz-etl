import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from etl.transform.flatten_transformer import FlattenTransformer

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestFlattenTransformer").getOrCreate()

def test_flatten_struct_fields(spark):
    schema = StructType([
        StructField("user", StructType([
            StructField("id", StringType()),
            StructField("name", StringType())
        ]))
    ])
    data = [{"user": {"id": "u1", "name": "Alice"}}]
    df = spark.createDataFrame(data, schema=schema)

    flat_df = FlattenTransformer(df).transform()

    assert "user_id" in flat_df.columns
    assert "user_name" in flat_df.columns
    result = flat_df.collect()[0].asDict()
    assert result["user_id"] == "u1"
    assert result["user_name"] == "Alice"

def test_flatten_array_of_structs(spark):
    schema = StructType([
        StructField("songs", ArrayType(StructType([
            StructField("title", StringType()),
            StructField("genre", StringType())
        ])))
    ])
    data = [{"songs": [{"title": "Song1", "genre": "Rock"}, {"title": "Song2", "genre": "Pop"}]}]
    df = spark.createDataFrame(data, schema=schema)

    flat_df = FlattenTransformer(df).transform(explode_arrays=True)

    assert "songs_exploded_title" in flat_df.columns
    assert "songs_exploded_genre" in flat_df.columns
    rows = flat_df.collect()
    assert len(rows) == 2
    assert rows[0]["songs_exploded_title"] == "Song1"
    assert rows[1]["songs_exploded_title"] == "Song2"
