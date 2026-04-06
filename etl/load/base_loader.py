import duckdb
import logging
from pyspark.sql import DataFrame
from etl.utils.duck_db_writer import DuckDBWriter

logger = logging.getLogger(__name__)


class BaseLoader:
    """
    BaseLoader is a generic class for managing the creation of tables and loading data
    into DuckDB. Specific loaders for individual tables should inherit from this class.

    Attributes:
        _db_conn (duckdb.DuckDBPyConnection): Active DuckDB connection.
        _ddl (str): SQL DDL statement to create the target table.
        _table_name (str): Target table name.
        _columns (list): List of columns to write.
        _dedup_columns (list): List of columns used for deduplication during write.

    Methods:
        load_data(dataframe: DataFrame): Creates the table and writes the provided dataframe.
    """

    def __init__(self,
                 db_conn: duckdb.DuckDBPyConnection,
                 ddl: str,
                 table_name: str,
                 columns: list,
                 dedup_columns: list):
        """
        Initialize BaseLoader with the necessary information for table creation and data loading.

        Args:
            db_conn (duckdb.DuckDBPyConnection): Active DuckDB connection.
            ddl (str): SQL statement to create the target table.
            table_name (str): Name of the table in DuckDB.
            columns (list): Columns to write to the table.
            dedup_columns (list): Columns to use for deduplication.
        """
        self._db_conn = db_conn
        self._ddl = ddl
        self._table_name = table_name
        self._columns = columns
        self._dedup_columns = dedup_columns

    def _create_table(self):
        """
        Executes the DDL to create the target table in DuckDB.

        Raises:
            duckdb.Error: If there is a failure executing the DDL.
        """
        try:
            logger.info(f"Executing DDL to create table {self._table_name}:\n{self._ddl}")
            self._db_conn.execute(self._ddl)
            logger.info(f"Table {self._table_name} created successfully.")
        except duckdb.Error as e:
            logger.error(f"Error creating table {self._table_name}: {e}", exc_info=True)
            raise

    def _write_data(self, dataframe: DataFrame):
        """
        Writes data from the Spark DataFrame into the DuckDB table, using DuckDBWriter.

        Args:
            dataframe (DataFrame): Spark DataFrame to write.

        Raises:
            Exception: Propagates exceptions from DuckDBWriter.
        """
        try:
            writer = DuckDBWriter(self._db_conn)
            writer.write(
                dataframe,
                self._table_name,
                self._columns,
                self._dedup_columns
            )
            logger.info(f"Data written successfully to table {self._table_name}.")
        except Exception as e:
            logger.error(f"Failed to write data to table {self._table_name}: {e}", exc_info=True)
            raise

    def load_data(self, dataframe: DataFrame):
        """
        Creates the target table (if not exists) and loads the given dataframe into it.

        Args:
            dataframe (DataFrame): Spark DataFrame to load into DuckDB.

        Raises:
            Exception: Propagates any exceptions from table creation or data write.
        """
        self._create_table()
        self._write_data(dataframe)
