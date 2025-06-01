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
         all_cats.Product_Category,
         COALESCE(SUM(jan_sales.Total_Amount), 0) AS category_revenue
     FROM
         (SELECT DISTINCT Product_Category FROM Retails) all_cats
     LEFT JOIN
         (SELECT Product_Category, Total_Amount
          FROM Retails
          WHERE Year = 2024 AND Month = 'January' AND Order_Status IN ('COMPLETE', 'CLOSED')
         ) jan_sales
     ON all_cats.Product_Category = jan_sales.Product_Category
     GROUP BY
         all_cats.Product_Category
     ORDER BY
         all_cats.Product_Category ASC
""")
# Hiển thị kết quả
result.show(50)