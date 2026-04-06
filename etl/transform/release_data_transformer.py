import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, array, sha2
from pyspark.sql.types import ArrayType, StringType

from etl.constants import Constants

logger = logging.getLogger(__name__)


class ReleaseDataTransformer:
    """
    Transformer class to extract and transform release dimension data
    from a nested input DataFrame.
    """

    # Required columns for schema validation
    REQUIRED_COLUMNS = {
        "track_metadata_release_name",
        "track_metadata_additional_info_release_msid",
        "track_metadata_additional_info_release_mbid",
        "track_metadata_additional_info_release_group_mbid",
        "track_metadata_additional_info_release_artist_name",
        "track_metadata_additional_info_release_artist_names",
        "track_metadata_additional_info_spotify_album_id",
        "track_metadata_additional_info_spotify_album_artist_ids",
        "track_metadata_additional_info_discnumber",
        "track_metadata_additional_info_totaldiscs",
        "track_metadata_additional_info_totaltracks"
    }

    def __init__(self, dataframe: DataFrame):
        """
        Initialize the transformer with the input DataFrame.

        Args:
            dataframe (DataFrame): Raw input DataFrame containing release metadata.
        """
        self.__dataframe = dataframe

    def _validate_schema(self):
        """
        Validates the presence of required columns in the DataFrame.

        Raises:
            ValueError: If any required columns are missing.
        """
        missing = self.REQUIRED_COLUMNS - set(self.__dataframe.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        logger.debug("Schema validation passed.")

    def transform(self) -> DataFrame:
        """
        Transforms the raw release metadata into a flattened dimension table.

        Returns:
            DataFrame: Transformed DataFrame containing release dimension data.

        Raises:
            Exception: If the transformation fails due to invalid schema or logic error.
        """
        try:
            logger.info("Starting transformation for release data.")
            self._validate_schema()

            dim_release = (
                self.__dataframe.select(
                    col("track_metadata_release_name").alias(Constants.SQL.TABLE.DIM_RELEASE.RELEASE_NAME),
                    col("track_metadata_additional_info_release_msid").alias(Constants.SQL.TABLE.DIM_RELEASE.RELEASE_MSID),
                    col("track_metadata_additional_info_release_mbid").alias(Constants.SQL.TABLE.DIM_RELEASE.RELEASE_MBID),
                    col("track_metadata_additional_info_release_group_mbid").alias(Constants.SQL.TABLE.DIM_RELEASE.RELEASE_GROUP_MBID),
                    col("track_metadata_additional_info_release_artist_name").alias(Constants.SQL.TABLE.DIM_RELEASE.RELEASE_ARTIST_NAME),
                    coalesce(
                        col("track_metadata_additional_info_release_artist_names"),
                        array().cast(ArrayType(StringType()))
                    ).alias(Constants.SQL.TABLE.DIM_RELEASE.RELEASE_ARTIST_NAMES),
                    col("track_metadata_additional_info_spotify_album_id").alias(Constants.SQL.TABLE.DIM_RELEASE.SPOTIFY_ALBUM_ID),
                    coalesce(
                        col("track_metadata_additional_info_spotify_album_artist_ids"),
                        array().cast(ArrayType(StringType()))
                    ).alias(Constants.SQL.TABLE.DIM_RELEASE.SPOTIFY_ALBUM_ARTIST_IDS),
                    col("track_metadata_additional_info_discnumber").alias(Constants.SQL.TABLE.DIM_RELEASE.DISC_NUMBER),
                    col("track_metadata_additional_info_totaldiscs").alias(Constants.SQL.TABLE.DIM_RELEASE.TOTAL_DISCS),
                    col("track_metadata_additional_info_totaltracks").alias(Constants.SQL.TABLE.DIM_RELEASE.TOTAL_TRACKS)
                )
                .dropna(subset=[Constants.SQL.TABLE.DIM_RELEASE.RELEASE_MSID])
                .dropDuplicates()
            )

            dim_release = dim_release.withColumn(
                Constants.SQL.TABLE.DIM_RELEASE.RELEASE_ID,
                sha2(col(Constants.SQL.TABLE.DIM_RELEASE.RELEASE_MSID), 256)
            )

            logger.info("Release data transformation completed successfully.")
            return dim_release

        except Exception as e:
            logger.error("Failed to transform release data", exc_info=True)
            raise
