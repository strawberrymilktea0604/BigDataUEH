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
SELECT VendorID, trip_distance, 
       TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) as duration_minutes,
       ROUND((trip_distance / NULLIF(TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0, 0)), 2) as speed_mph
FROM tripdata 
WHERE trip_distance > 0 
AND TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) > 0
AND (trip_distance / NULLIF(TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0, 0)) > 80 
ORDER BY speed_mph DESC
LIMIT 100;
""")

# Hiển thị kết quả
result.show(30)