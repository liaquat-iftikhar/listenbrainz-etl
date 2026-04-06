WITH abc AS (
    SELECT track_id, COUNT(*) AS user_count
    FROM listenbrainz_etl.main.fact_listen_event
    WHERE CAST(TO_TIMESTAMP(listened_at) AS DATE) = DATE '2019-03-01'
    GROUP BY 1
    ORDER BY 2 DESC
)
SELECT dt.track_name, abc.user_count
FROM listenbrainz_etl.main.dim_track dt
INNER JOIN abc
    ON abc.track_id = dt.track_id
WHERE user_count > 1
ORDER BY 2 DESC;