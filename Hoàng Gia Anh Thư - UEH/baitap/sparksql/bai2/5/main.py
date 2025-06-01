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
        r.Product_Category AS department_name, -- Assuming Product_Category is the department
        COUNT(DISTINCT r.products) AS product_count
    FROM
        Retails r
    GROUP BY
        r.Product_Category
    ORDER BY
        department_name ASC
""")

# Hiển thị kết quả
result.show(50)