SELECT
    CASE
        WHEN user_gender = 'M' THEN 'Male'
        WHEN user_gender = 'F' THEN 'Female'
        ELSE 'Not Specified'
    END AS user_gender,
    COUNT(user_id) AS user_count
FROM
    users
GROUP BY
    CASE
        WHEN user_gender = 'M' THEN 'Male'
        WHEN user_gender = 'F' THEN 'Female'
        ELSE 'Not Specified'
    END
ORDER BY
    user_count DESC;