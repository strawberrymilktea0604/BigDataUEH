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
SELECT Country, Region, `Area (sq. mi.)`
FROM countries 
WHERE `Area (sq. mi.)` > 0
ORDER BY `Area (sq. mi.)` ASC 
LIMIT 5;
""")

result2 = spark.sql("""
SELECT Country, Region, `Area (sq. mi.)`
FROM countries 
ORDER BY `Area (sq. mi.)` DESC 
LIMIT 5;
""")

# Hiển thị kết quả
result.show(50)
result2.show(50)