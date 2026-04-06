import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, regexp_replace

from etl.constants import Constants

logger = logging.getLogger(__name__)


class ListenEventDataTransformer:
    """
    Transformer class to prepare the fact table for listen events.

    This class processes raw listening event data and generates hashed identifiers
    for user, track, artist, release, and additional info. It also cleans user names
    and removes duplicates and nulls from key fields.
    """

    # Define required columns for schema validation
    REQUIRED_COLUMNS = {
        "listened_at",
        "user_name",
        "track_metadata_additional_info_recording_msid",
        "track_metadata_additional_info_artist_msid",
        "track_metadata_additional_info_release_msid",
        "track_metadata_additional_info_duration_ms",
        "track_metadata_additional_info_source",
        "track_metadata_additional_info_rating"
    }

    def __init__(self, dataframe: DataFrame):
        """
        Initializes the ListenEventDataTransformer with the input DataFrame.

        Args:
            dataframe (DataFrame): The input raw listen event data.
        """
        self.__dataframe = dataframe

    def _validate_schema(self):
        """
        Validates that the DataFrame contains all required columns.

        Raises:
            ValueError: If any required columns are missing.
        """
        missing_columns = self.REQUIRED_COLUMNS - set(self.__dataframe.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns in DataFrame: {missing_columns}")
        logger.debug("Schema validation passed")

    def transform(self) -> DataFrame:
        """
        Transforms the input DataFrame into the fact_listen_events schema with necessary
        hashed IDs and cleaned fields.

        Returns:
            DataFrame: The transformed fact_listen_events DataFrame.

        Raises:
            Exception: If transformation fails.
        """
        try:
            logger.info("Starting transformation of listen event data")

            # Validate schema
            self._validate_schema()

            fact_listen_events = self.__dataframe.select(
                col("listened_at").alias(Constants.SQL.TABLE.FACT_LISTEN_EVENT.LISTENED_AT),
                sha2(
                    regexp_replace(col("user_name"), r"[^\w\s]", ""), 256
                ).alias(Constants.SQL.TABLE.FACT_LISTEN_EVENT.USER_ID),
                sha2(
                    col("track_metadata_additional_info_recording_msid"), 256
                ).alias(Constants.SQL.TABLE.FACT_LISTEN_EVENT.ADDITIONAL_INFO_ID),
                sha2(
                    col("track_metadata_additional_info_recording_msid"), 256
                ).alias(Constants.SQL.TABLE.FACT_LISTEN_EVENT.TRACK_ID),
                sha2(
                    col("track_metadata_additional_info_artist_msid"), 256
                ).alias(Constants.SQL.TABLE.FACT_LISTEN_EVENT.ARTIST_ID),
                sha2(
                    col("track_metadata_additional_info_release_msid"), 256
                ).alias(Constants.SQL.TABLE.FACT_LISTEN_EVENT.RELEASE_ID),
                col("track_metadata_additional_info_duration_ms").alias(
                    Constants.SQL.TABLE.FACT_LISTEN_EVENT.DURATION_MS
                ),
                col("track_metadata_additional_info_source").alias(
                    Constants.SQL.TABLE.FACT_LISTEN_EVENT.SOURCE
                ),
                col("track_metadata_additional_info_rating").alias(
                    Constants.SQL.TABLE.FACT_LISTEN_EVENT.RATING
                )
            )

            # Drop rows with nulls in key deduplication columns
            fact_listen_events = fact_listen_events.dropna(
                subset=Constants.SQL.TABLE.FACT_LISTEN_EVENT.DEDUP_COLUMNS
            ).dropDuplicates()

            logger.info("Successfully transformed listen event data")
            return fact_listen_events

        except Exception as e:
            logger.error("Error during listen event transformation", exc_info=True)
            raise
