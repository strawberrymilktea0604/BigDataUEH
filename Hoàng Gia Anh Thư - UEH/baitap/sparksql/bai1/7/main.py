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
SELECT Country, Region, `Infant mortality (per 1000 births)`
FROM countries 
WHERE Region IN ('SUB-SAHARAN AFRICA                 ', 'NORTHERN AFRICA                    ')
  AND `Infant mortality (per 1000 births)` IS NOT NULL
ORDER BY CAST(REPLACE(`Infant mortality (per 1000 births)`, ',', '.') AS DECIMAL(10,2)) DESC 
LIMIT 5;
""")

# Hiển thị kết quả
result.show(50)