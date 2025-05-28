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
SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, 
       TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) as duration_minutes
FROM tripdata 
WHERE tpep_pickup_datetime > tpep_dropoff_datetime
ORDER BY duration_minutes DESC;
""")

# Hiển thị kết quả
result.show(50)