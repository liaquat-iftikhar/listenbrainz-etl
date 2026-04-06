from etl.load.base_loader import BaseLoader
from etl.constants import Constants


class DimArtistLoader(BaseLoader):

    def __init__(self, db_conn):
        super().__init__(
            db_conn=db_conn,
            ddl=Constants.SQL.DDL.DIM_ARTIST,
            table_name=Constants.SQL.TABLE.DIM_ARTIST.NAME,
            columns=Constants.SQL.TABLE.DIM_ARTIST.COLUMNS,
            dedup_columns=Constants.SQL.TABLE.DIM_ARTIST.DEDUP_COLUMNS
        )
