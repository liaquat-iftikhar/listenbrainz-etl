import json
import pytest
from pyspark.sql import SparkSession
from etl.extract.reader import Reader
from etl.config import Config


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


@pytest.fixture
def test_input_file(tmp_path):
    sample_data = [
        {
            "listened_at": 1622520000,
            "recording_msid": "msid123",
            "track_metadata": {
                "additional_info": {
                    "albumartist": "Artist A",
                    "artist_mbids": ["mbid1", "mbid2"],
                    "artist_msid": "artist_msid_123",
                    "artist_names": ["Artist A", "Artist A feat. Artist B"],
                    "choosen_by_user": 1,
                    "date": "2021-06-01",
                    "dedup_tag": 0,
                    "discnumber": "1",
                    "duration": "3:45",
                    "duration_ms": 225000,
                    "isrc": "USRC17607839",
                    "listening_from": "mobile",
                    "rating": "5",
                    "recording_mbid": "rec_mbid_123",
                    "recording_msid": "rec_msid_123",
                    "release_artist_name": "Artist A",
                    "release_artist_names": ["Artist A"],
                    "release_group_mbid": "rg_mbid_123",
                    "release_mbid": "rel_mbid_123",
                    "release_msid": "rel_msid_123",
                    "source": "spotify",
                    "spotify_album_artist_ids": ["spotify_artist_1"],
                    "spotify_album_id": "spotify_album_1",
                    "spotify_artist_ids": ["spotify_artist_1", "spotify_artist_2"],
                    "spotify_id": "spotify_track_1",
                    "tags": ["tag1", "tag2"],
                    "totaldiscs": "1",
                    "totaltracks": "10",
                    "track_length": "3:45",
                    "track_mbid": "track_mbid_123",
                    "track_number": "3",
                    "tracknumber": "3",
                    "work_mbids": ["work_mbid_1"]
                },
                "artist_name": "Artist A",
                "release_name": "Release X",
                "track_name": "Track 1"
            },
            "user_name": "user_1"
        },
        {
            "listened_at": 1622606400,
            "recording_msid": "msid456",
            "track_metadata": {
                "additional_info": {
                    "albumartist": "Artist B",
                    "artist_mbids": ["mbid3"],
                    "artist_msid": "artist_msid_456",
                    "artist_names": ["Artist B"],
                    "choosen_by_user": 0,
                    "date": "2021-06-02",
                    "dedup_tag": 1,
                    "discnumber": "1",
                    "duration": "4:00",
                    "duration_ms": 240000,
                    "isrc": "USRC17607840",
                    "listening_from": "desktop",
                    "rating": "4",
                    "recording_mbid": "rec_mbid_456",
                    "recording_msid": "rec_msid_456",
                    "release_artist_name": "Artist B",
                    "release_artist_names": ["Artist B"],
                    "release_group_mbid": "rg_mbid_456",
                    "release_mbid": "rel_mbid_456",
                    "release_msid": "rel_msid_456",
                    "source": "spotify",
                    "spotify_album_artist_ids": ["spotify_artist_3"],
                    "spotify_album_id": "spotify_album_2",
                    "spotify_artist_ids": ["spotify_artist_3"],
                    "spotify_id": "spotify_track_2",
                    "tags": ["tag3"],
                    "totaldiscs": "1",
                    "totaltracks": "12",
                    "track_length": "4:00",
                    "track_mbid": "track_mbid_456",
                    "track_number": "5",
                    "tracknumber": "5",
                    "work_mbids": ["work_mbid_2"]
                },
                "artist_name": "Artist B",
                "release_name": "Release Y",
                "track_name": "Track 2"
            },
            "user_name": "user_2"
        }
    ]

    file_path = tmp_path / "sample_input.json"

    # Convert list to JSON string with one JSON object per line (JSON Lines format)
    with open(file_path, "w", encoding="utf-8") as f:
        for record in sample_data:
            f.write(json.dumps(record) + "\n")

    # Patch Config.INPUT_FILE_PATH temporarily
    original_path = Config.INPUT_FILE_PATH
    Config.INPUT_FILE_PATH = str(file_path)

    yield file_path

    Config.INPUT_FILE_PATH = original_path


def test_read_data_success(spark, test_input_file):
    reader = Reader(spark)
    df = reader.read_data()

    assert df.count() == 2
    assert "recording_msid" in df.columns
