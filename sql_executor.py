import os
import pandas as pd
from sqlalchemy import create_engine
from etl.config import Config


class SQLReportRunner:
    def __init__(self, sql_dir: str):
        """
        Initializes the SQLReportRunner.
        :param sql_dir: Directory containing SQL query files.
        """
        self.sql_dir = sql_dir
        self.db_path = Config.OUTPUT_FILE_PATH

        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database file not found: {self.db_path}")

        try:
            self.engine = create_engine(f'duckdb:///{self.db_path}')
        except Exception as e:
            raise RuntimeError(f"Failed to create database engine. Path: {self.db_path}") from e

        self._configure_pandas()

    @staticmethod
    def _configure_pandas():
        """Configure pandas display options to show all columns and rows."""
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

    def _load_query(self, filename: str) -> str:
        """
        Loads an SQL query from a file.
        :param filename: Name of the SQL file.
        :return: SQL query string.
        """
        filepath = os.path.join(self.sql_dir, filename)

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"SQL file not found: {filepath}")

        with open(filepath, 'r') as file:
            return file.read()

    def _run_query(self, filename: str, message: str, limit: int = 10):
        """
        Executes a SQL query from a file and displays the result.
        :param filename: SQL file to read from.
        :param message: Message to print before the result.
        :param limit: Number of rows to display.
        """
        print("\n" + ("-" * 100))
        print(message)
        print("-" * 100 + "\n")

        try:
            query = self._load_query(filename)
            df = pd.read_sql_query(query, self.engine)
            print(df.head(limit))
        except Exception as e:
            print(f"Failed to execute query from {filename}. Error: {e}")

    def run_all(self):
        """Runs all predefined SQL reports."""
        reports = [
            ('1_top_users.sql', "1. Top 10 users by number of songs listened to:"),
            ('2_same_song_on_date.sql', "2. Users who listened to the same song on 1st March 2019:"),
            ('3_first_song_per_user.sql', "3. First song each user listened to:"),
            ('4_top_listening_days.sql', "4. Top 3 listening days per user:"),
            ('5_active_user_trend.sql', "5. Active user trend and percentage:")
        ]

        for filename, message in reports:
            self._run_query(filename, message)


if __name__ == "__main__":
    try:
        runner = SQLReportRunner(sql_dir='sql')
        runner.run_all()
    except Exception as main_err:
        print(f"An error occurred while running SQL reports: {main_err}")
