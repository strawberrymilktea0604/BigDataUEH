SELECT
    SUBSTRING(user_phone_no, 2, CHARINDEX(' ', user_phone_no) - 3) AS country_code,
    COUNT(*) AS user_count
FROM
    users
WHERE
    user_phone_no IS NOT NULL
    AND CHARINDEX(' ', user_phone_no) > 2 -- Đảm bảo có dấu cách sau mã quốc gia
GROUP BY
    SUBSTRING(user_phone_no, 2, CHARINDEX(' ', user_phone_no) - 3)
ORDER BY
    CAST(SUBSTRING(user_phone_no, 2, CHARINDEX(' ', user_phone_no) - 3) AS INT) ASC;