from etl.constants import Constants
from etl.load.base_loader import BaseLoader


class DimReleaseLoader(BaseLoader):

    def __init__(self, db_conn):
        super().__init__(
            db_conn=db_conn,
            ddl=Constants.SQL.DDL.DIM_RELEASE,
            table_name=Constants.SQL.TABLE.DIM_RELEASE.NAME,
            columns=Constants.SQL.TABLE.DIM_RELEASE.COLUMNS,
            dedup_columns=Constants.SQL.TABLE.DIM_RELEASE.DEDUP_COLUMNS
        )
