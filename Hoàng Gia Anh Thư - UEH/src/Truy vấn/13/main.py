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
       COUNT(CASE WHEN passenger_count = 0 THEN 1 END) as zero_passenger_trips,
       COUNT(CASE WHEN trip_distance = 0 THEN 1 END) as zero_distance_trips,
       COUNT(CASE WHEN total_amount <= 0 THEN 1 END) as zero_or_negative_amount,
       COUNT(CASE WHEN store_and_fwd_flag NOT IN ('Y', 'N') THEN 1 END) as invalid_store_flag,
       COUNT(CASE WHEN RateCodeID NOT BETWEEN 1 AND 6 THEN 1 END) as invalid_rate_code,
       ROUND(COUNT(CASE WHEN passenger_count = 0 THEN 1 END) * 100.0 / COUNT(*), 2) as zero_passenger_percentage
FROM tripdata 
GROUP BY VendorID
ORDER BY VendorID;
""")

# Hiển thị kết quả
result.show(50)