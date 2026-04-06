from etl.load.base_loader import BaseLoader
from etl.constants import Constants


class FactListenEventLoader(BaseLoader):

    def __init__(self, db_conn):
        super().__init__(
            db_conn=db_conn,
            ddl=Constants.SQL.DDL.FACT_LISTEN_EVENT,
            table_name=Constants.SQL.TABLE.FACT_LISTEN_EVENT.NAME,
            columns=Constants.SQL.TABLE.FACT_LISTEN_EVENT.COLUMNS,
            dedup_columns=Constants.SQL.TABLE.FACT_LISTEN_EVENT.DEDUP_COLUMNS
        )
