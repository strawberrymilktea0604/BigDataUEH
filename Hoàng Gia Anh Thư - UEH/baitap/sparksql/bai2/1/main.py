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
        r.Customer_ID AS customer_id,
        split(r.Name, ' ', 2)[0] AS customer_first_name,
        split(r.Name, ' ', 2)[1] AS customer_last_name,
        COUNT(DISTINCT r.Transaction_ID) AS customer_order_count
    FROM
        Retails r
    WHERE
        r.Year = 2024 AND r.Month = 'January'
    GROUP BY
        r.Customer_ID, r.Name
    ORDER BY
        customer_order_count DESC, customer_id ASC
""")

# Hiển thị kết quả
result.show(50)