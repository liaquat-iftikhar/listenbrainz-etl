WITH all_dates AS (
    SELECT DISTINCT CAST(TO_TIMESTAMP(listened_at) AS DATE) AS date
    FROM listenbrainz_etl.main.fact_listen_event
),
active_users_per_day AS (
    SELECT
        d.date AS current_date,
        le.user_id
    FROM all_dates d
    JOIN listenbrainz_etl.main.fact_listen_event le
        ON CAST(TO_TIMESTAMP(listened_at) AS DATE) BETWEEN d.date - INTERVAL 6 DAY AND d.date
    GROUP BY d.date, le.user_id
),
active_user_counts AS (
    SELECT
        current_date AS date,
        COUNT(DISTINCT user_id) AS number_active_users
    FROM active_users_per_day
    GROUP BY current_date
),
total_users AS (
    SELECT COUNT(DISTINCT user_id) AS total_users
    FROM listenbrainz_etl.main.fact_listen_event
)
SELECT
    a.date,
    a.number_active_users,
    ROUND((a.number_active_users * 100.0) / t.total_users, 2) AS percentage_active_users
FROM active_user_counts a
CROSS JOIN total_users t
ORDER BY a.date;
