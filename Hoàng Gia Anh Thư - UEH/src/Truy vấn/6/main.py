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
SELECT DATE(tpep_pickup_datetime) as pickup_date,
       store_and_fwd_flag,
       COUNT(*) as trip_count,
       AVG(trip_distance) as avg_distance,
       AVG(Total_amount) as avg_amount
FROM tripdata 
WHERE store_and_fwd_flag IN ('Y', 'N')
GROUP BY DATE(tpep_pickup_datetime), store_and_fwd_flag
HAVING trip_count > 10
ORDER BY pickup_date, store_and_fwd_flag;
""")

# Hiển thị kết quả
result.show(30)