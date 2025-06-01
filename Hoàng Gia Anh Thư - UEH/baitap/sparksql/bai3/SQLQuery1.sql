SELECT
    YEAR(created_ts) AS created_year,
    COUNT(user_id) AS user_count
FROM
    users
GROUP BY
    YEAR(created_ts)
ORDER BY
    created_year ASC;