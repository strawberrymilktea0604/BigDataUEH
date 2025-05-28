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
WITH tip_stats AS (
    SELECT AVG(tip_amount / NULLIF(fare_amount, 0)) as avg_tip_ratio,
           STDDEV(tip_amount / NULLIF(fare_amount, 0)) as stddev_tip_ratio
    FROM tripdata 
    WHERE payment_type = 1 AND fare_amount > 0 AND tip_amount >= 0
)
SELECT t.VendorID, t.tpep_pickup_datetime, t.fare_amount, t.tip_amount,
       ROUND((t.tip_amount / NULLIF(t.fare_amount, 0)), 3) as tip_ratio,
       ROUND((t.tip_amount / NULLIF(t.fare_amount, 0) - ts.avg_tip_ratio) / NULLIF(ts.stddev_tip_ratio, 0), 2) as z_score
FROM tripdata t
CROSS JOIN tip_stats ts
WHERE t.payment_type = 1 
AND t.fare_amount > 0 
AND t.tip_amount >= 0
AND ABS((t.tip_amount / NULLIF(t.fare_amount, 0) - ts.avg_tip_ratio) / NULLIF(ts.stddev_tip_ratio, 0)) > 3
ORDER BY z_score DESC
LIMIT 50;
""")

# Hiển thị kết quả
result.show(50)