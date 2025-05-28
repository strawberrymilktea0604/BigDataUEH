import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Dừng tất cả SparkContext đang chạy
SparkContext.setSystemProperty("spark.executor.memory", "2g")
SparkContext.setSystemProperty("spark.driver.memory", "2g")

spark = SparkSession.builder \
    .appName("TripDataAnalysis") \
    .master("local[*]") \
    .getOrCreate()


# Đọc dữ liệu từ HDFS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/BigData/yellow_tripdata_2016-01.csv")
    
df.createOrReplaceTempView("tripdata")

# Thực hiện truy vấn Spark SQL
result = spark.sql("""
SELECT VendorID, payment_type, fare_amount, tip_amount,
       ROUND((tip_amount / NULLIF(fare_amount, 0)) * 100, 2) as tip_percentage
FROM tripdata 
WHERE fare_amount > 0 
AND payment_type = 1
AND (tip_amount / NULLIF(fare_amount, 0)) > 0.5  
ORDER BY tip_percentage DESC
LIMIT 100;
""")

# Hiển thị kết quả
result.show(30)