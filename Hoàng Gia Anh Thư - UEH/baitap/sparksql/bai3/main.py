import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, length, col, when
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder \
    .appName("SQLServerRetail") \
    .config("spark.driver.extraClassPath", "C:/spark/jars/mssql-jdbc-12.10.0.jre8.jar") \
    .getOrCreate()
    
    

jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=retails;encrypt=true;trustServerCertificate=true"
connection_properties = {
    "user": "lakhanh231",
    "password": "vuphuong123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

users_df = spark.read.jdbc(url=jdbc_url, table="users", properties=connection_properties)
users_df.createOrReplaceTempView("users")

spark.sql("""
SELECT
    YEAR(created_ts) AS created_year,
    COUNT(*) AS user_count
FROM users
GROUP BY YEAR(created_ts)
ORDER BY created_year ASC
""").show()

spark.sql("""
SELECT
    user_id,
    user_dob,
    user_email_id,
    CONCAT(
        UPPER(LEFT(date_format(user_dob, 'EEEE'), 1)),
        LOWER(SUBSTRING(date_format(user_dob, 'EEEE'), 2, LENGTH(date_format(user_dob, 'EEEE')) - 1))
    ) AS user_day_of_birth
FROM users
WHERE MONTH(user_dob) = 6
ORDER BY DAY(user_dob)
""").show()

spark.sql("""
SELECT
    user_id,
    UPPER(CONCAT(user_first_name, ' ', user_last_name)) AS user_name,
    user_email_id,
    created_ts,
    YEAR(created_ts) AS created_year
FROM users
WHERE YEAR(created_ts) = 2019
ORDER BY user_name ASC
""").show()

spark.sql("""
SELECT
    CASE
        WHEN user_gender = 'M' THEN 'Male'
        WHEN user_gender = 'F' THEN 'Female'
        ELSE 'Not Specified'
    END AS user_gender,
    COUNT(*) AS user_count
FROM users
GROUP BY
    CASE
        WHEN user_gender = 'M' THEN 'Male'
        WHEN user_gender = 'F' THEN 'Female'
        ELSE 'Not Specified'
    END
ORDER BY user_count DESC
""").show()



users_df = users_df.withColumn(
    "digits_only",
    regexp_replace(col("user_unique_id"), "-", "")
)

users_df = users_df.withColumn(
    "user_unique_id_last4",
    when(col("user_unique_id").isNull(), "Not Specified")
    .when(length(col("digits_only")) < 9, "Invalid Unique Id")
    .otherwise(col("digits_only").substr(-4, 4))
)

users_df.select(
    "user_id", "user_unique_id", "user_unique_id_last4"
).orderBy("user_id").show(25, False)


users_df = users_df.withColumn(
    "country_code",
    regexp_extract(col("user_phone_no"), r"\+(\d+)", 1)
)

users_df.filter(col("country_code") != "") \
    .groupBy("country_code") \
    .count() \
    .orderBy(col("country_code").cast("int").asc()) \
    .withColumnRenamed("count", "user_count") \
    .show()
    
order_items_df = spark.read.jdbc(url=jdbc_url, table="order_items", properties=connection_properties)
order_items_df.createOrReplaceTempView("order_items")

spark.sql("""
SELECT COUNT(*) AS invalid_count
FROM order_items
WHERE ROUND(order_item_quantity * order_item_product_price, 2) != ROUND(order_item_subtotal, 2)
""").show()

orders_df = spark.read.jdbc(url=jdbc_url, table="orders", properties=connection_properties)
orders_df.createOrReplaceTempView("orders")

spark.sql("""
WITH day_types AS (
    SELECT 'Week days' AS day_type
    UNION ALL
    SELECT 'Weekend days' AS day_type
),
orders_count AS (
    SELECT
        CASE
            WHEN dayofweek(order_date) IN (1,7) THEN 'Weekend days'
            ELSE 'Week days'
        END AS day_type,
        COUNT(*) AS order_count
    FROM orders
    WHERE YEAR(order_date) = 2014 AND MONTH(order_date) = 1
    GROUP BY
        CASE
            WHEN dayofweek(order_date) IN (1,7) THEN 'Weekend days'
            ELSE 'Week days'
        END
)
SELECT
    d.day_type,
    COALESCE(o.order_count, 0) AS order_count
FROM day_types d
LEFT JOIN orders_count o ON d.day_type = o.day_type
""").show()