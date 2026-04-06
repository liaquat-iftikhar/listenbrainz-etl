import logging

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    col, coalesce, sha2, lit, filter, length,
    regexp_replace, when, size, array
)
from pyspark.sql.types import ArrayType, StringType

from etl.constants import Constants

logger = logging.getLogger(__name__)


class AdditionalInfoDataTransformer:
    """
    Transforms a Spark DataFrame to create a cleaned and enriched version of the
    dim_additional_info dimension table.

    The transformation includes:
    - Aliasing and coalescing null values
    - Filtering non-null and meaningful values in array fields
    - Generating a SHA-256 hash-based surrogate key

    Attributes:
        __dataframe (DataFrame): Input Spark DataFrame containing raw additional info data.

    Methods:
        transform() -> DataFrame:
            Performs transformations and returns the cleaned DataFrame.
    """

    REQUIRED_COLUMNS = {
        "track_metadata_additional_info_recording_msid",
        "track_metadata_additional_info_recording_mbid",
        "track_metadata_additional_info_tags",
        "track_metadata_additional_info_work_mbids",
        "track_metadata_additional_info_listening_from",
        "track_metadata_additional_info_choosen_by_user",
        "track_metadata_additional_info_dedup_tag",
        "track_metadata_additional_info_date",
        "track_metadata_additional_info_source",
    }

    def __init__(self, dataframe: DataFrame):
        """
        Initializes the transformer with the raw input DataFrame.

        Args:
            dataframe (DataFrame): Raw input DataFrame.
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

    # noinspection PyTypeChecker
    @staticmethod
    def clean_array_column(col_name: str) -> Column:
        """
        Cleans an array column by:
        - Filtering out null or empty elements (after removing non-alphanumeric characters)
        - Replacing empty arrays with an array containing a single empty string

        This is particularly useful for sanitizing arrays such as tags or IDs
        to ensure only meaningful values are retained and the output always conforms
        to expected schema structure (i.e., non-null array type).

        Args:
            col_name (str): The name of the column to clean.

        Returns:
            Column: A Spark Column expression that returns a cleaned array.
        """
        return when(
            size(
                filter(
                    col(col_name),
                    lambda x: (x.isNotNull()) & (length(regexp_replace(x, r"\W+", "")) > 0)
                )
            ) > 0,
            filter(
                col(col_name),
                lambda x: (x.isNotNull()) & (length(regexp_replace(x, r"\W+", "")) > 0)
            )
        ).otherwise(array(lit('')))

    def transform(self) -> DataFrame:
        """
        Transforms the input DataFrame into the dim_additional_info structure
        by selecting, cleaning, filtering, and hashing appropriate fields.

        Returns:
            DataFrame: Transformed DataFrame for loading into dim_additional_info.

        Raises:
            Exception: Logs and re-raises any exceptions that occur during transformation.
        """
        try:
            logger.info("Starting transformation for dim_additional_info")

            self._validate_schema()

            # Step 1: Select and alias required columns, coalesce nulls
            dim_additional_info = (
                self.__dataframe.select(
                    col("track_metadata_additional_info_recording_msid")
                        .alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.RECORDING_MSID),

                    coalesce(col("track_metadata_additional_info_recording_mbid"), lit(''))
                        .alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.RECORDING_MBID),

                    coalesce(
                        col("track_metadata_additional_info_tags"),
                        lit([]).cast(ArrayType(StringType()))
                    ).alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.TAGS),

                    coalesce(
                        col("track_metadata_additional_info_work_mbids"),
                        lit([]).cast(ArrayType(StringType()))
                    ).alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.WORK_MBIDS),

                    coalesce(col("track_metadata_additional_info_listening_from"), lit(''))
                        .alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.LISTENING_FROM),

                    col("track_metadata_additional_info_choosen_by_user")
                        .alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.CHOOSEN_BY_USER),

                    col("track_metadata_additional_info_dedup_tag")
                        .alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.DEDUP_TAG),

                    coalesce(col("track_metadata_additional_info_date"), lit(''))
                        .alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.DATE),

                    coalesce(col("track_metadata_additional_info_source"), lit(''))
                        .alias(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.SOURCE)
                )
                .dropna(subset=[Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.RECORDING_MSID])
                .dropDuplicates()
            )

            logger.info("Initial selection and cleaning completed.")

            # Step 2: Add surrogate key using SHA-256
            dim_additional_info = dim_additional_info.withColumn(
                Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.ADDITIONAL_INFO_ID,
                sha2(
                    col(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.RECORDING_MSID),
                    256
                )
            )

            # Step 3: Clean and validate TAGS and WORK_MBIDS array fields
            dim_additional_info = dim_additional_info.withColumn(
                Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.TAGS,
                self.clean_array_column(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.TAGS)
            ).withColumn(
                Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.WORK_MBIDS,
                self.clean_array_column(Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.WORK_MBIDS)
            )

            logger.info("Transformation for dim_additional_info completed successfully.")
            return dim_additional_info

        except Exception:
            logger.error("Error occurred during AdditionalInfoDataTransformer.transform()", exc_info=True)
            raise
