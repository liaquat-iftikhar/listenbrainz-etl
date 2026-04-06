import os


class Config:

    # Resolve input file path relative to the current file
    CURRENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    INPUT_FILE_PATH = os.path.join(CURRENT_DIR, 'data', 'dataset.txt')
    OUTPUT_FILE_PATH = os.path.abspath(os.path.join(CURRENT_DIR, 'output', 'listenbrainz_etl.duckdb'))
