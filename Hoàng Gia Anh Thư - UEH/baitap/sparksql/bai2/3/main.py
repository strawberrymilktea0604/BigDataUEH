import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Dừng tất cả SparkContext đang chạy
SparkContext.setSystemProperty("spark.executor.memory", "2g")
SparkContext.setSystemProperty("spark.driver.memory", "2g")

spark = SparkSession.builder \
    .appName("Bai2") \
    .master("local[*]") \
    .getOrCreate()


# Đọc dữ liệu từ HDFS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/BigData/new_retail_data.csv")
    
df.createOrReplaceTempView("Retails")

# Thực hiện truy vấn Spark SQL
result = spark.sql("""
SELECT
        cust.Customer_ID AS customer_id,
        split(cust.Name, ' ', 2)[0] AS customer_first_name,
        split(cust.Name, ' ', 2)[1] AS customer_last_name,
        COALESCE(SUM(orders.Total_Amount), 0) AS customer_revenue
    FROM
        (SELECT DISTINCT Customer_ID, Name FROM Retails) cust
    LEFT JOIN
        (SELECT
             Customer_ID,
             Total_Amount
         FROM
             Retails
         WHERE
             Year = 2024 AND
             Month = 'January' AND
             Order_Status IN ('COMPLETE', 'CLOSED')
        ) orders
    ON cust.Customer_ID = orders.Customer_ID
    GROUP BY
        cust.Customer_ID, cust.Name
    ORDER BY
        customer_revenue DESC, customer_id ASC
""")

# Hiển thị kết quả
result.show(50)