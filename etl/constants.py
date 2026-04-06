class Constants:

    class SQL:

        class TABLE:

            class DIM_USER:
                NAME = "dim_user"

                # Define each column name as a separate constant
                USER_ID = "user_id"
                USER_NAME = "user_name"

                # Use the constants to build lists
                DEDUP_COLUMNS = [USER_ID]
                COLUMNS = [USER_ID, USER_NAME]

            class DIM_ARTIST:
                NAME = "dim_artist"

                # Define each column name as a separate constant
                ARTIST_ID = "artist_id"
                ARTIST_NAME = "artist_name"
                ARTIST_MSID = "artist_msid"
                ARTIST_MBIDS = "artist_mbids"
                SPOTIFY_ARTIST_IDS = "spotify_artist_ids"

                # Use the constants to build lists
                DEDUP_COLUMNS = [ARTIST_ID]
                COLUMNS = [ARTIST_ID, ARTIST_NAME, ARTIST_MSID, ARTIST_MBIDS, SPOTIFY_ARTIST_IDS]

            class DIM_RELEASE:
                NAME = "dim_release"

                # Define each column name as a separate constant
                RELEASE_ID = "release_id"
                RELEASE_NAME = "release_name"
                RELEASE_MSID = "release_msid"
                RELEASE_MBID = "release_mbid"
                RELEASE_GROUP_MBID = "release_group_mbid"
                RELEASE_ARTIST_NAME = "release_artist_name"
                RELEASE_ARTIST_NAMES = "release_artist_names"
                SPOTIFY_ALBUM_ID = "spotify_album_id"
                SPOTIFY_ALBUM_ARTIST_IDS = "spotify_album_artist_ids"
                DISC_NUMBER = "disc_number"
                TOTAL_DISCS = "total_discs"
                TOTAL_TRACKS = "total_tracks"

                # Use the constants to build lists
                DEDUP_COLUMNS = [RELEASE_ID]
                COLUMNS = [RELEASE_ID, RELEASE_NAME, RELEASE_MSID,RELEASE_MBID, RELEASE_GROUP_MBID,
                           RELEASE_ARTIST_NAME, RELEASE_ARTIST_NAMES, SPOTIFY_ALBUM_ID, SPOTIFY_ALBUM_ARTIST_IDS,
                           DISC_NUMBER, TOTAL_DISCS, TOTAL_TRACKS]

            class DIM_TRACK:
                NAME = "dim_track"

                # Define each column name as a separate constant
                TRACK_ID = "track_id"
                TRACK_NAME = "track_name"
                TRACK_MSID = "track_msid"
                TRACK_MBID = "track_mbid"
                TRACK_NUMBER = "track_number"
                DURATION = "duration"
                DURATION_MS = "duration_ms"
                ISRC = "isrc"
                TRACK_LENGTH = "track_length"

                # Use the constants to build lists
                DEDUP_COLUMNS = [TRACK_ID]
                COLUMNS = [TRACK_ID, TRACK_NAME, TRACK_MSID, TRACK_MBID, TRACK_NUMBER, DURATION, DURATION_MS, ISRC,
                           TRACK_LENGTH]

            class DIM_ADDITIONAL_INFO:
                NAME = "dim_additional_info"

                # Define each column name as a separate constant
                ADDITIONAL_INFO_ID = "additional_info_id"
                RECORDING_MSID = "recording_msid"
                RECORDING_MBID = "recording_mbid"
                TAGS = "tags"
                WORK_MBIDS = "work_mbids"
                LISTENING_FROM = "listening_from"
                CHOOSEN_BY_USER = "choosen_by_user"
                DEDUP_TAG = "dedup_tag"
                DATE = "date"
                SOURCE = "source"

                # Use the constants to build lists
                DEDUP_COLUMNS = [ADDITIONAL_INFO_ID]
                COLUMNS = [ADDITIONAL_INFO_ID, RECORDING_MSID, RECORDING_MBID, TAGS, WORK_MBIDS, LISTENING_FROM,
                           CHOOSEN_BY_USER, DEDUP_TAG, DATE, SOURCE]

            class FACT_LISTEN_EVENT:
                NAME = "fact_listen_event"

                # Define each column name as a separate constant
                LISTENED_AT = "listened_at"
                USER_ID = "user_id"
                ADDITIONAL_INFO_ID = "additional_info_id"
                TRACK_ID = "track_id"
                ARTIST_ID = "artist_id"
                RELEASE_ID = "release_id"
                DURATION_MS = "duration_ms"
                SOURCE = "source"
                RATING = "rating"

                # Use the constants to build lists
                DEDUP_COLUMNS = [USER_ID, ADDITIONAL_INFO_ID, TRACK_ID, ARTIST_ID, RELEASE_ID, LISTENED_AT]
                COLUMNS = [LISTENED_AT, USER_ID, ADDITIONAL_INFO_ID, TRACK_ID, ARTIST_ID, RELEASE_ID, DURATION_MS,
                           SOURCE, RATING]

        class DDL:

            DIM_USER = """
                CREATE TABLE IF NOT EXISTS dim_user (
                    user_id VARCHAR PRIMARY KEY,
                    user_name VARCHAR
                )
            """

            DIM_ARTIST = """
                CREATE TABLE IF NOT EXISTS dim_artist (
                    artist_id VARCHAR PRIMARY KEY,
                    artist_name VARCHAR,
                    artist_msid VARCHAR,
                    artist_mbids VARCHAR[],
                    spotify_artist_ids VARCHAR[]
                )
            """

            DIM_RELEASE = """
                            CREATE TABLE IF NOT EXISTS dim_release (
                                release_id VARCHAR PRIMARY KEY,
                                release_name VARCHAR,
                                release_msid VARCHAR,
                                release_mbid VARCHAR,
                                release_group_mbid VARCHAR,
                                release_artist_name VARCHAR,
                                release_artist_names VARCHAR[],
                                spotify_album_id VARCHAR,
                                spotify_album_artist_ids VARCHAR[],
                                disc_number VARCHAR,
                                total_discs VARCHAR,
                                total_tracks VARCHAR,
                            )
                        """

            DIM_TRACK = """
                            CREATE TABLE IF NOT EXISTS dim_track (
                                track_id VARCHAR PRIMARY KEY,
                                track_name VARCHAR,
                                track_msid VARCHAR,
                                track_mbid VARCHAR,
                                track_number VARCHAR,
                                duration VARCHAR,
                                duration_ms LONG,
                                isrc VARCHAR,
                                track_length VARCHAR
                            )
                        """

            DIM_ADDITIONAL_INFO = """
                                    CREATE TABLE IF NOT EXISTS dim_additional_info (
                                        additional_info_id VARCHAR PRIMARY KEY,
                                        recording_msid VARCHAR,
                                        recording_mbid VARCHAR,
                                        tags VARCHAR[],
                                        work_mbids VARCHAR[],
                                        listening_from VARCHAR,
                                        choosen_by_user LONG,
                                        dedup_tag LONG,
                                        date VARCHAR,
                                        source VARCHAR
                                    )
                                    """

            FACT_LISTEN_EVENT = """
                                    CREATE TABLE IF NOT EXISTS fact_listen_event (
                                        listened_at  LONG,
                                        user_id VARCHAR,
                                        additional_info_id VARCHAR,
                                        track_id VARCHAR,
                                        artist_id VARCHAR,
                                        release_id VARCHAR,
                                        duration_ms LONG,
                                        source VARCHAR,
                                        rating VARCHAR,
                                        PRIMARY KEY (user_id, additional_info_id, track_id, artist_id, release_id,
                                                     listened_at)
                                    )
                                """
