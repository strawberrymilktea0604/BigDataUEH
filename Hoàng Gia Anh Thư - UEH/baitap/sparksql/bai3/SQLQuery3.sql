SELECT
    user_id,
    UPPER(CONCAT_WS(' ', user_first_name, user_last_name)) AS user_name,
    user_email_id,
    created_ts,
    YEAR(created_ts) AS created_year
FROM
    users
WHERE
    YEAR(created_ts) = 2019
ORDER BY
    user_name ASC;