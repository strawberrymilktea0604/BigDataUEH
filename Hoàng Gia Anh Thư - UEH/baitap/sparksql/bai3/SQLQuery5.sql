WITH CleanedUsers AS (
    SELECT
        user_id,
        user_unique_id,
        REPLACE(user_unique_id, '-', '') AS cleaned_id
    FROM users
)
SELECT
    user_id,
    user_unique_id,
    CASE
        WHEN user_unique_id IS NULL THEN 'Not Specified'
        WHEN LEN(cleaned_id) < 9 THEN 'Invalid Unique Id'
        ELSE RIGHT(cleaned_id, 4)
    END AS user_unique_id_last4
FROM
    CleanedUsers
ORDER BY
    user_id ASC;