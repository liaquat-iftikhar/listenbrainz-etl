WITH count_listen_event AS (
    SELECT user_id,
           CAST(TO_TIMESTAMP(listened_at) AS DATE) AS listened_at,
           COUNT(*) AS listen_count
    FROM listenbrainz_etl.main.fact_listen_event
    GROUP BY 1, 2
), row_num_count_listen_event AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY listen_count DESC, listened_at ASC) AS row_num
    FROM count_listen_event
)
SELECT du.user_name, le.listened_at, le.listen_count
FROM row_num_count_listen_event le
INNER JOIN listenbrainz_etl.main.dim_user du
    ON le.user_id = du.user_id
WHERE row_num <= 3
ORDER BY 1, 3 ASC;
