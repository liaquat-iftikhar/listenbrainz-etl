from etl.constants import Constants
from etl.load.base_loader import BaseLoader


class DimTrackLoader(BaseLoader):
    def __init__(self, db_conn):
        super().__init__(
            db_conn=db_conn,
            ddl=Constants.SQL.DDL.DIM_TRACK,
            table_name=Constants.SQL.TABLE.DIM_TRACK.NAME,
            columns=Constants.SQL.TABLE.DIM_TRACK.COLUMNS,
            dedup_columns=Constants.SQL.TABLE.DIM_TRACK.DEDUP_COLUMNS
        )
