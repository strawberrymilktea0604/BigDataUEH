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
SELECT DATE(tpep_pickup_datetime) as trip_date,
       COUNT(*) as jfk_trips,
       AVG(trip_distance) as avg_distance,
       MIN(trip_distance) as min_distance,
       MAX(trip_distance) as max_distance,
       AVG(total_amount) as avg_total,
       COUNT(CASE WHEN trip_distance < 10 THEN 1 END) as suspicious_short_trips
FROM tripdata 
WHERE RateCodeID = 2
AND trip_distance > 0
GROUP BY DATE(tpep_pickup_datetime)
HAVING jfk_trips > 5
ORDER BY trip_date;
""")

# Hiển thị kết quả
result.show(50)