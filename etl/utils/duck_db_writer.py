import logging
from typing import List

import duckdb
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DuckDBWriter:
    """
    A utility class for writing PySpark DataFrames to DuckDB with idempotency support using ON CONFLICT handling.

    Attributes:
        __db_conn (duckdb.DuckDBPyConnection): An active DuckDB database connection.
    """

    def __init__(self, db_conn: duckdb.DuckDBPyConnection):
        """
        Initializes the writer with an active DuckDB connection.

        Args:
            db_conn (duckdb.DuckDBPyConnection): A valid and open DuckDB connection.
        """
        self.__db_conn = db_conn

    def write(self, dataframe: DataFrame, table_name: str, columns: List[str], dedup_columns: List[str]):
        """
        Writes a DataFrame to the specified DuckDB table using an idempotent insert strategy.
        It uses DuckDB's ON CONFLICT clause to avoid inserting duplicate rows based on deduplication keys.

        Args:
            dataframe (DataFrame): The PySpark DataFrame to be written.
            table_name (str): The name of the target DuckDB table.
            columns (List[str]): The list of columns to insert into.
            dedup_columns (List[str]): The list of columns to use for deduplication (ON CONFLICT constraint).

        Raises:
            Exception: If the insertion fails due to any database or conversion issue.
        """
        try:
            logger.info(f"Preparing to write DataFrame to table: {table_name}")

            # Convert Spark DataFrame to Pandas DataFrame for DuckDB compatibility
            pandas_df = dataframe.toPandas()
            pandas_df = pandas_df[columns]

            # Register temporary DuckDB table
            self.__db_conn.register("tmp_df", pandas_df)
            logger.debug(f"Registered temporary table 'tmp_df' with columns: {columns}")

            # Build and execute deduplicated insert query
            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)}) 
                SELECT {', '.join(columns)} 
                FROM tmp_df
                ON CONFLICT ({', '.join(dedup_columns)}) DO NOTHING
                RETURNING *;
            """

            logger.info(f"Executing DuckDB insert query: {query.strip()}")

            result = self.__db_conn.execute(query).fetchall()
            logger.info(f"{len(result)} rows inserted into table '{table_name}' successfully.")

            # Clean up temporary table
            self.__db_conn.unregister("tmp_df")

        except Exception as e:
            logger.error(f"Failed to write DataFrame to DuckDB table '{table_name}'.", exc_info=True)
            raise
