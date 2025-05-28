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
SELECT passenger_count,
       COUNT(*) as trip_count,
       AVG(trip_distance) as avg_distance,
       AVG(total_amount) as avg_total_amount,
       AVG(tip_amount) as avg_tip,
       ROUND(AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100, 2) as avg_tip_percentage
FROM tripdata 
WHERE passenger_count BETWEEN 1 AND 6
AND payment_type = 1
AND fare_amount > 0
GROUP BY passenger_count
ORDER BY passenger_count;
""")

# Hiển thị kết quả
result.show(50)