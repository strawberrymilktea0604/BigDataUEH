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
SELECT payment_type,
       COUNT(*) as trip_count,
       AVG(extra) as avg_extra,
       AVG(mta_tax) as avg_mta_tax,
       AVG(improvement_surcharge) as avg_improvement_surcharge,
       AVG(tolls_amount) as avg_tolls
FROM tripdata 
WHERE extra >= 0 AND extra <= 10 
AND mta_tax >= 0 AND mta_tax <= 2
AND improvement_surcharge >= 0 AND improvement_surcharge <= 1
AND tolls_amount >= 0 AND tolls_amount <= 50
GROUP BY payment_type
ORDER BY payment_type;
""")

# Hiển thị kết quả
result.show(50)