import os
import logging

from pyspark.sql import SparkSession

from etl.config import Config
from etl.listen_brainz_etl import ListenBrainzETL
from etl.utils.duck_db_connector import DuckDBConnector


# Set up global logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def main():
    """
    Main entry point for executing the ListenBrainz ETL pipeline.

    It initializes Spark, sets up a DuckDB connection, and runs the ETL process.
    Ensures proper error handling and graceful shutdown of the DuckDB connection.
    """
    spark = None
    db = None

    try:
        logger.info("Starting ListenBrainz ETL pipeline...")

        # Initialize Spark session
        spark = (SparkSession.builder
                 .appName("ListenBrainzETL")
                 .getOrCreate())
        logger.info("Spark session initialized.")

        # Connect to DuckDB
        db = DuckDBConnector(Config.OUTPUT_FILE_PATH)
        conn = db.get_connection()

        # Run ETL
        ListenBrainzETL(spark, conn).run()
        logger.info("ETL pipeline completed successfully.")

    except Exception as e:
        logger.error("An error occurred during the ETL pipeline execution.", exc_info=True)
        raise

    finally:
        if db:
            db.close()
            logger.info("DuckDB connection closed.")
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == '__main__':
    main()
