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
SELECT VendorID,
       COUNT(*) as total_trips,
       AVG(Trip_distance) as avg_distance,
       AVG(Total_amount) as avg_total_amount,
       AVG(TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)) as avg_duration_minutes
FROM tripdata 
WHERE Trip_distance BETWEEN 0.1 AND 100
AND Total_amount BETWEEN 0 AND 500
AND TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 1 AND 300 
GROUP BY VendorID;
""")

# Hiển thị kết quả
result.show(50)