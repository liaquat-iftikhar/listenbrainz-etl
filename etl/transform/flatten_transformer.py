import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

logger = logging.getLogger(__name__)


class FlattenTransformer:
    """
    A transformer class to flatten nested PySpark DataFrames.

    This class handles the transformation of DataFrames containing nested
    StructType and optionally ArrayType[StructType] fields into a flat schema.
    It is useful when working with complex JSON or deeply nested data structures.

    Attributes:
        __dataframe (DataFrame): The input PySpark DataFrame to be flattened.
    """

    def __init__(self, dataframe: DataFrame):
        """
        Initializes the FlattenTransformer with the input DataFrame.

        Args:
            dataframe (DataFrame): A PySpark DataFrame to flatten.
        """
        self.__dataframe = dataframe

    def transform(self, explode_arrays: bool = False) -> DataFrame:
        """
        Flattens a nested PySpark DataFrame by expanding all StructType columns into individual columns.
        Optionally explodes array of struct columns into separate rows and flattens them as well.

        Args:
            explode_arrays (bool): If True, explode array<struct> columns and flatten them.

        Returns:
            DataFrame: Flattened PySpark DataFrame.

        Raises:
            ValueError: If input is not a PySpark DataFrame.
            Exception: Propagates unexpected exceptions encountered during flattening.
        """
        try:
            df = self.__dataframe

            if not hasattr(df, "schema"):
                raise ValueError("Input is not a valid PySpark DataFrame")

            logger.info("Starting flattening of DataFrame.")
            iteration = 0

            while True:
                iteration += 1
                logger.debug(f"Flattening iteration: {iteration}")

                # Identify struct columns
                struct_cols = [
                    (field.name, field.dataType)
                    for field in df.schema.fields
                    if isinstance(field.dataType, StructType)
                ]

                # Identify array<struct> columns if explode_arrays is True
                array_struct_cols = [
                    (field.name, field.dataType)
                    for field in df.schema.fields
                    if isinstance(field.dataType, ArrayType)
                    and isinstance(field.dataType.elementType, StructType)
                ]

                if not struct_cols and (not explode_arrays or not array_struct_cols):
                    logger.debug("No more nested structs or arrays to flatten/explode. Exiting loop.")
                    break

                # Flatten struct columns
                for col_name, struct_type in struct_cols:
                    logger.debug(f"Flattening struct column: {col_name}")
                    expanded = [
                        col(f"{col_name}.{field.name}").alias(f"{col_name}_{field.name}")
                        for field in struct_type.fields
                    ]
                    other_cols = [col(c) for c in df.columns if c != col_name]
                    df = df.select(*other_cols, *expanded)

                # Explode and flatten array<struct> columns
                if explode_arrays:
                    for col_name, array_type in array_struct_cols:
                        logger.debug(f"Exploding and flattening array<struct> column: {col_name}")
                        exploded_col = f"{col_name}_exploded"
                        df = df.withColumn(exploded_col, explode_outer(col(col_name)))
                        expanded = [
                            col(f"{exploded_col}.{field.name}").alias(f"{exploded_col}_{field.name}")
                            for field in array_type.elementType.fields
                        ]
                        other_cols = [col(c) for c in df.columns if c != col_name]
                        df = df.select(*other_cols, *expanded)

            logger.info("DataFrame successfully flattened.")
            return df

        except Exception as e:
            logger.error("Error while flattening DataFrame", exc_info=True)
            raise
