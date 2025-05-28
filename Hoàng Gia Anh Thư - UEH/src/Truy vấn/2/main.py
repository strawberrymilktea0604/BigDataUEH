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
       AVG(passenger_count) as avg_passengers,
       COUNT(*) as total_trips,
       STDDEV(passenger_count) as passenger_stddev
FROM tripdata 
WHERE passenger_count BETWEEN 1 AND 6
AND trip_distance > 0
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY pickup_hour;
""")

# Hiển thị kết quả
result.show(50)