import logging
from typing import Optional

import duckdb


class DuckDBConnector:
    """
    Manages the lifecycle of a DuckDB connection with support for in-memory or persistent file-based databases.

    Attributes:
        db_path (str): Path to the DuckDB database file. Use ':memory:' for an in-memory instance.
        conn (duckdb.DuckDBPyConnection): Active DuckDB connection object.
        logger (logging.Logger): Logger for connection-related messages.
    """

    def __init__(self, db_path: str = ":memory:"):
        """
        Initializes a DuckDBConnector instance with the given database path.

        Args:
            db_path (str): Path to DuckDB database. Use ':memory:' for temporary in-memory usage.
        """
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """
        Establishes a connection to the DuckDB database.

        Raises:
            duckdb.IOException: If the connection cannot be established.
        """
        try:
            self.conn = duckdb.connect(self.db_path)
            self.logger.info(f"DuckDB connection established at {self.db_path}")
        except Exception as e:
            self.logger.error("Failed to connect to DuckDB.", exc_info=True)
            raise

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Returns the active DuckDB connection. If no connection exists, it is created.

        Returns:
            duckdb.DuckDBPyConnection: Active DuckDB connection.

        Raises:
            duckdb.IOException: If connection fails.
        """
        if self.conn is None:
            self.logger.debug("No existing connection found. Attempting to connect...")
            self.connect()
        return self.conn

    def close(self):
        """
        Closes the DuckDB connection if it is active.

        Logs a warning if an attempt is made to close a non-existent connection.
        """
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.logger.info("DuckDB connection closed successfully.")
            else:
                self.logger.warning("Attempted to close DuckDB connection, but none was open.")
        except Exception as e:
            self.logger.error("Error occurred while closing DuckDB connection.", exc_info=True)
            raise
