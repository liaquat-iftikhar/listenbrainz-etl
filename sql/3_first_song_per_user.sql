WITH listen_event_row_number AS (
    SELECT
        user_id,
        track_id,
        listened_at,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY listened_at ASC) AS rn
    FROM listenbrainz_etl.main.fact_listen_event
)
SELECT du.user_name, dt.track_name, TO_TIMESTAMP(le.listened_at) AS first_listen_time
FROM listenbrainz_etl.main.dim_track dt
INNER JOIN listen_event_row_number le
    ON dt.track_id = le.track_id
INNER JOIN listenbrainz_etl.main.dim_user du
    ON le.user_id = du.user_id
WHERE le.rn = 1;
