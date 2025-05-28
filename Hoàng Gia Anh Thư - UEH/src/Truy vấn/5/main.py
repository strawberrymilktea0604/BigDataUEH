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
SELECT COUNT(*) as invalid_coordinates,
       COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tripdata) as percentage
FROM tripdata 
WHERE pickup_latitude NOT BETWEEN 40.4 AND 41.0
OR pickup_longitude NOT BETWEEN -74.3 AND -73.7
OR dropoff_latitude NOT BETWEEN 40.4 AND 41.0
OR dropoff_longitude NOT BETWEEN -74.3 AND -73.7
OR pickup_latitude = 0 OR pickup_longitude = 0
OR dropoff_latitude = 0 OR dropoff_longitude = 0;
""")

# Hiển thị kết quả
result.show(50)