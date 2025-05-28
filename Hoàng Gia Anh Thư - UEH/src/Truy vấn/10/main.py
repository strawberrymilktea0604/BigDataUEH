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
SELECT VendorID, fare_amount, extra, mta_tax, improvement_surcharge, 
       tip_amount, tolls_amount, total_amount,
       ROUND((fare_amount + extra + mta_tax + improvement_surcharge + tip_amount + tolls_amount), 2) as calculated_total,
       ROUND(ABS(total_amount - (fare_amount + extra + mta_tax + improvement_surcharge + tip_amount + tolls_amount)), 2) as difference
FROM tripdata 
WHERE ABS(total_amount - (fare_amount + extra + mta_tax + improvement_surcharge + tip_amount + tolls_amount)) > 0.10
AND total_amount > 0
ORDER BY difference DESC
LIMIT 100
""")

# Hiển thị kết quả
result.show(50)