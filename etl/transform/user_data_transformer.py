import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, regexp_replace

from etl.constants import Constants

logger = logging.getLogger(__name__)


class UserDataTransformer:
    """
    Transformer class to process user data into a dimension table format.
    """

    REQUIRED_COLUMNS = {"user_name"}

    def __init__(self, dataframe: DataFrame):
        """
        Initialize the transformer with the input DataFrame.

        Args:
            dataframe (DataFrame): Input DataFrame containing user data.
        """
        self.__dataframe = dataframe

    def _validate_schema(self):
        """
        Validates that all required columns are present in the input DataFrame.

        Raises:
            ValueError: If required columns are missing.
        """
        missing_cols = self.REQUIRED_COLUMNS - set(self.__dataframe.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        logger.debug("Schema validation passed.")

    def transform(self) -> DataFrame:
        """
        Transforms the input user data into a cleaned dimension table with user IDs.

        Returns:
            DataFrame: Transformed DataFrame with cleaned user names and generated user IDs.

        Raises:
            Exception: If transformation or validation fails.
        """
        try:
            logger.info("Starting user data transformation.")
            self._validate_schema()

            dim_user = (
                self.__dataframe
                .select(Constants.SQL.TABLE.DIM_USER.USER_NAME)
                .dropna()
                .dropDuplicates()
                .withColumn(
                    Constants.SQL.TABLE.DIM_USER.USER_NAME,
                    regexp_replace(col(Constants.SQL.TABLE.DIM_USER.USER_NAME), r"[^\w\s]", "")
                )
                .withColumn(
                    Constants.SQL.TABLE.DIM_USER.USER_ID,
                    sha2(col(Constants.SQL.TABLE.DIM_USER.USER_NAME), 256)
                )
            )

            logger.info("User data transformation completed successfully.")
            return dim_user

        except Exception as e:
            logger.error("Error during user data transformation.", exc_info=True)
            raise
