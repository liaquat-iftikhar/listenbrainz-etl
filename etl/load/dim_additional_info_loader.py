from etl.load.base_loader import BaseLoader
from etl.constants import Constants


class DimAdditionalInfoLoader(BaseLoader):

    def __init__(self, db_conn):
        super().__init__(
            db_conn=db_conn,
            ddl=Constants.SQL.DDL.DIM_ADDITIONAL_INFO,
            table_name=Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.NAME,
            columns=Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.COLUMNS,
            dedup_columns=Constants.SQL.TABLE.DIM_ADDITIONAL_INFO.DEDUP_COLUMNS
        )
