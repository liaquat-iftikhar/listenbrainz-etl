import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, regexp_replace

from etl.constants import Constants

logger = logging.getLogger(__name__)


class TrackDataTransformer:
    """
    Transformer class to process and transform track metadata into a dimensional table format.
    """

    REQUIRED_COLUMNS = {
        "track_metadata_track_name",
        "track_metadata_additional_info_recording_msid",
        "track_metadata_additional_info_track_mbid",
        "track_metadata_additional_info_track_number",
        "track_metadata_additional_info_duration",
        "track_metadata_additional_info_duration_ms",
        "track_metadata_additional_info_isrc",
        "track_metadata_additional_info_track_length"
    }

    def __init__(self, dataframe: DataFrame):
        """
        Initialize the transformer with the input DataFrame.

        Args:
            dataframe (DataFrame): Raw input DataFrame containing track metadata.
        """
        self.__dataframe = dataframe

    def _validate_schema(self):
        """
        Validate that all required columns are present in the DataFrame.

        Raises:
            ValueError: If any required columns are missing.
        """
        missing = self.REQUIRED_COLUMNS - set(self.__dataframe.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        logger.debug("Schema validation passed.")

    def transform(self) -> DataFrame:
        """
        Transform the raw track metadata into a flattened dimension table.

        Returns:
            DataFrame: Transformed DataFrame with cleaned track metadata and generated TRACK_ID.

        Raises:
            Exception: For any errors during transformation or schema validation.
        """
        try:
            logger.info("Starting transformation for track data.")
            self._validate_schema()

            dim_track = (
                self.__dataframe.select(
                    col("track_metadata_track_name").alias(Constants.SQL.TABLE.DIM_TRACK.TRACK_NAME),
                    col("track_metadata_additional_info_recording_msid").alias(Constants.SQL.TABLE.DIM_TRACK.TRACK_MSID),
                    col("track_metadata_additional_info_track_mbid").alias(Constants.SQL.TABLE.DIM_TRACK.TRACK_MBID),
                    col("track_metadata_additional_info_track_number").alias(Constants.SQL.TABLE.DIM_TRACK.TRACK_NUMBER),
                    col("track_metadata_additional_info_duration").alias(Constants.SQL.TABLE.DIM_TRACK.DURATION),
                    col("track_metadata_additional_info_duration_ms").alias(Constants.SQL.TABLE.DIM_TRACK.DURATION_MS),
                    col("track_metadata_additional_info_isrc").alias(Constants.SQL.TABLE.DIM_TRACK.ISRC),
                    col("track_metadata_additional_info_track_length").alias(Constants.SQL.TABLE.DIM_TRACK.TRACK_LENGTH)
                )
                .dropna(subset=[Constants.SQL.TABLE.DIM_TRACK.TRACK_MSID])
                .dropDuplicates()
            )

            dim_track = (
                dim_track.withColumn(
                    Constants.SQL.TABLE.DIM_TRACK.TRACK_NAME,
                    regexp_replace(col(Constants.SQL.TABLE.DIM_TRACK.TRACK_NAME), r"[^\w\s]", "")
                )
                .withColumn(
                    Constants.SQL.TABLE.DIM_TRACK.TRACK_ID,
                    sha2(col(Constants.SQL.TABLE.DIM_TRACK.TRACK_MSID), 256)
                )
            )

            logger.info("Track data transformation completed successfully.")
            return dim_track

        except Exception as e:
            logger.error("Failed to transform track data", exc_info=True)
            raise
