import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Dừng tất cả SparkContext đang chạy
SparkContext.setSystemProperty("spark.executor.memory", "2g")
SparkContext.setSystemProperty("spark.driver.memory", "2g")

spark = SparkSession.builder \
    .appName("Bai1") \
    .master("local[*]") \
    .getOrCreate()


# Đọc dữ liệu từ HDFS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/BigData/countries.csv")
    
df.createOrReplaceTempView("countries")

# Thực hiện truy vấn Spark SQL
result = spark.sql("""
SELECT Region, 
       AVG(CAST(`GDP ($ per capita)` AS DECIMAL(10,0))) AS Average_GDP,
       COUNT(*) AS Number_of_Countries
FROM countries 
WHERE `GDP ($ per capita)` IS NOT NULL AND `GDP ($ per capita)` > 0
GROUP BY Region 
ORDER BY Average_GDP DESC 
LIMIT 1;
""")

# Hiển thị kết quả
result.show(50)