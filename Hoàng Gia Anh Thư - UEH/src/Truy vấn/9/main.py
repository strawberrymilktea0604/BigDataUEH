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
SELECT trip_distance, fare_amount, total_amount,
       ROUND(fare_amount / NULLIF(trip_distance, 0), 2) as fare_per_mile,
       RateCodeID, VendorID
FROM tripdata 
WHERE trip_distance BETWEEN 0.1 AND 2 
AND fare_amount > 20  
AND RateCodeID = 1  
ORDER BY fare_per_mile DESC
LIMIT 50;
""")

# Hiển thị kết quả
result.show(50)