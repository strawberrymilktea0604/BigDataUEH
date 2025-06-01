SELECT
    user_id,
    user_dob,
    user_email_id,
    DATENAME(weekday, user_dob) AS user_day_of_birth
FROM
    users
WHERE
    MONTH(user_dob) = 6
ORDER BY
    DAY(user_dob) ASC;