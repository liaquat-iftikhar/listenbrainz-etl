WITH top_ten_listener AS (
    SELECT user_id, COUNT(*) AS songs_count
    FROM listenbrainz_etl.main.fact_listen_event fle
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
)
SELECT du.user_name, tl.songs_count
FROM listenbrainz_etl.main.dim_user du
INNER JOIN top_ten_listener tl
    ON du.user_id = tl.user_id
ORDER BY 2 DESC;
