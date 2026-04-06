import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, array, sha2
from pyspark.sql.types import ArrayType, StringType

from etl.constants import Constants

logger = logging.getLogger(__name__)


class ArtistDataTransformer:
    """
    Transforms raw input DataFrame into a cleaned and deduplicated dimension table
    for artist information, ready to be loaded into DuckDB.
    """

    # Required columns to be present in the input DataFrame
    REQUIRED_COLUMNS = {
        "track_metadata_artist_name",
        "track_metadata_additional_info_artist_msid",
        "track_metadata_additional_info_artist_mbids",
        "track_metadata_additional_info_spotify_artist_ids",
    }

    def __init__(self, dataframe: DataFrame):
        """
        Initializes the transformer with the source DataFrame.

        Args:
            dataframe (DataFrame): The raw Spark DataFrame containing artist metadata.
        """
        self.__dataframe = dataframe

    def _validate_schema(self):
        """
        Validates that the input DataFrame contains all required columns.

        Raises:
            ValueError: If required columns are missing.
        """
        missing_cols = self.REQUIRED_COLUMNS - set(self.__dataframe.columns)
        if missing_cols:
            raise ValueError(f"Input DataFrame missing required columns: {missing_cols}")
        logger.debug("Input DataFrame schema validation passed.")

    def transform(self) -> DataFrame:
        """
        Transforms the input DataFrame by selecting and renaming relevant columns,
        cleaning null values, handling missing arrays, and generating a unique artist ID.

        Returns:
            DataFrame: A cleaned and enriched DataFrame for the artist dimension table.

        Raises:
            Exception: For any unexpected errors during transformation.
        """
        try:
            logger.info("Starting transformation for Artist dimension.")
            self._validate_schema()

            # Select and clean artist metadata fields
            dim_artist = (
                self.__dataframe.select(
                    col("track_metadata_artist_name").alias(Constants.SQL.TABLE.DIM_ARTIST.ARTIST_NAME),
                    col("track_metadata_additional_info_artist_msid").alias(Constants.SQL.TABLE.DIM_ARTIST.ARTIST_MSID),
                    col("track_metadata_additional_info_artist_mbids").alias(Constants.SQL.TABLE.DIM_ARTIST.ARTIST_MBIDS),
                    coalesce(
                        col("track_metadata_additional_info_spotify_artist_ids"),
                        array().cast(ArrayType(StringType()))
                    ).alias(Constants.SQL.TABLE.DIM_ARTIST.SPOTIFY_ARTIST_IDS)
                )
                .dropna(subset=[Constants.SQL.TABLE.DIM_ARTIST.ARTIST_MSID])
                .dropDuplicates()
            )

            logger.info("Artist metadata fields selected and cleaned.")

            # Add hashed ID for uniqueness
            dim_artist = dim_artist.withColumn(
                Constants.SQL.TABLE.DIM_ARTIST.ARTIST_ID,
                sha2(col(Constants.SQL.TABLE.DIM_ARTIST.ARTIST_MSID), 256)
            )

            logger.info("Artist ID column generated successfully.")
            return dim_artist

        except Exception as e:
            logger.error("Error during transformation for Artist dimension.", exc_info=True)
            raise
