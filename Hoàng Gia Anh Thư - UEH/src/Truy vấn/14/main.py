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
SELECT HOUR(tpep_pickup_datetime) as pickup_hour,
       COUNT(*) as total_trips,
       SUM(total_amount) as total_revenue,
       AVG(total_amount) as avg_fare_per_trip,
       SUM(tip_amount) as total_tips,
       ROUND(AVG(trip_distance), 2) as avg_distance
FROM tripdata 
WHERE total_amount > 0 AND trip_distance > 0
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY total_revenue DESC;
""")

# Hiển thị kết quả
result.show(50)