from etl.load.base_loader import BaseLoader
from etl.constants import Constants


class DimUserLoader(BaseLoader):

    def __init__(self, db_conn):
        super().__init__(
            db_conn=db_conn,
            ddl=Constants.SQL.DDL.DIM_USER,
            table_name=Constants.SQL.TABLE.DIM_USER.NAME,
            columns=Constants.SQL.TABLE.DIM_USER.COLUMNS,
            dedup_columns=Constants.SQL.TABLE.DIM_USER.DEDUP_COLUMNS
        )
