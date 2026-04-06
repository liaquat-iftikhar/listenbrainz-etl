import logging

import duckdb
from pyspark.sql import SparkSession

from etl.extract.reader import Reader
from etl.load.dim_additional_info_loader import DimAdditionalInfoLoader
from etl.load.dim_artist_loader import DimArtistLoader
from etl.load.dim_release_loader import DimReleaseLoader
from etl.load.dim_track_loader import DimTrackLoader
from etl.load.dim_user_loader import DimUserLoader
from etl.load.fact_listen_event_loader import FactListenEventLoader
from etl.transform.additional_info_data_transformer import AdditionalInfoDataTransformer
from etl.transform.artist_data_transformer import ArtistDataTransformer
from etl.transform.flatten_transformer import FlattenTransformer
from etl.transform.listen_event_data_transformer import ListenEventDataTransformer
from etl.transform.release_data_transformer import ReleaseDataTransformer
from etl.transform.track_data_transformer import TrackDataTransformer
from etl.transform.user_data_transformer import UserDataTransformer

logger = logging.getLogger(__name__)


class ListenBrainzETL:
    """
    Orchestrates the end-to-end ETL process for ListenBrainz data:
    - Reads JSON lines
    - Flattens and transforms data
    - Loads into DuckDB in dimensional model format
    """

    def __init__(self, spark: SparkSession, db_conn: duckdb.DuckDBPyConnection):
        """
        Initializes the ETL pipeline.

        Args:
            spark (SparkSession): The active Spark session.
            db_conn (duckdb.DuckDBPyConnection): The DuckDB connection for writing data.
        """
        self.__spark = spark
        self.__db_conn = db_conn

    def run(self):
        """
        Executes the ETL pipeline:
        1. Reads and flattens raw input data
        2. Transforms data into dimension/fact tables
        3. Loads each table into DuckDB
        """
        logger.info("ETL pipeline started.")
        try:
            # Extract
            reader = Reader(self.__spark)
            logger.info("Reading raw input data...")
            input_data = reader.read_data()
            logger.info("Raw data read successfully.")

            # Transform
            logger.info("Flattening raw data...")
            flatten_data = FlattenTransformer(input_data).transform()
            logger.info("Data flattening complete.")

            logger.info("Transforming to dimension and fact tables...")
            user_data = UserDataTransformer(flatten_data).transform()
            artist_data = ArtistDataTransformer(flatten_data).transform()
            release_data = ReleaseDataTransformer(flatten_data).transform()
            track_data = TrackDataTransformer(flatten_data).transform()
            additional_info_data = AdditionalInfoDataTransformer(flatten_data).transform()
            listen_event_data = ListenEventDataTransformer(flatten_data).transform()
            logger.info("All transformations complete.")

            # Load
            logger.info("Loading data into DuckDB...")
            DimUserLoader(self.__db_conn).load_data(user_data)
            DimArtistLoader(self.__db_conn).load_data(artist_data)
            DimReleaseLoader(self.__db_conn).load_data(release_data)
            DimTrackLoader(self.__db_conn).load_data(track_data)
            DimAdditionalInfoLoader(self.__db_conn).load_data(additional_info_data)
            FactListenEventLoader(self.__db_conn).load_data(listen_event_data)
            logger.info("All tables loaded successfully into DuckDB.")

        except Exception as e:
            logger.error("ETL pipeline failed.", exc_info=True)
            raise

        logger.info("ETL pipeline completed successfully.")
