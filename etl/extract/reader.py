import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from etl.extract.schema import listen_brainz_schema
from etl.config import Config


logger = logging.getLogger(__name__)


class Reader:
    """
    Loads JSON data into a Spark DataFrame using a provided SparkSession.

    Attributes:
        __spark_session (SparkSession): SparkSession used to read the data.
    """

    def __init__(self, spark_session: SparkSession):
        """
        Initialize Reader with the given SparkSession.

        Args:
            spark_session (SparkSession): SparkSession instance to use.
        """
        self.__spark_session = spark_session

    def __read_json(self) -> DataFrame:
        """
        Reads the JSON file with the configured schema and encoding.

        Returns:
            DataFrame: Parsed Spark DataFrame.

        Raises:
            IOError: If file reading fails.
            AnalysisException: If schema binding or parsing fails.
        """
        try:
            logger.info(f"Reading JSON data from: {Config.INPUT_FILE_PATH}")
            df = (
                self.__spark_session.read
                .option("encoding", "ascii")
                .option("lineSep", "\n")
                .schema(listen_brainz_schema)
                .json(Config.INPUT_FILE_PATH)
            )
            logger.info("Successfully read JSON data.")
            return df

        except AnalysisException as ae:
            logger.exception("Schema or Spark read failure.")
            raise ae

        except Exception as e:
            logger.exception("Unexpected error during JSON read.")
            raise IOError(f"Failed to read JSON data: {e}")

    def read_data(self) -> DataFrame:
        """
        Public interface to load the JSON data.

        Returns:
            DataFrame: Parsed Spark DataFrame.

        Raises:
            Exception: Propagates any exception from __read_json().
        """
        try:
            return self.__read_json()
        except Exception as e:
            logger.error("Error in read_data()")
            raise
