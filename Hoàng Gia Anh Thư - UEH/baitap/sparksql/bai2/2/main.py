import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Dừng tất cả SparkContext đang chạy
SparkContext.setSystemProperty("spark.executor.memory", "2g")
SparkContext.setSystemProperty("spark.driver.memory", "2g")

spark = SparkSession.builder \
    .appName("Bai2") \
    .master("local[*]") \
    .getOrCreate()


# Đọc dữ liệu từ HDFS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/BigData/new_retail_data.csv")
    
df.createOrReplaceTempView("Retails")

# Thực hiện truy vấn Spark SQL
result = spark.sql("""
    SELECT DISTINCT
         c.Customer_ID,
         split(c.Name, ' ', 2)[0] AS customer_first_name,
         split(c.Name, ' ', 2)[1] AS customer_last_name,
         c.Name AS original_customer_name,
         c.Email,
         c.Phone,
         c.Address,
         c.City,
         c.State,
         c.Zipcode,
         c.Country,
         c.Age,
         c.Gender,
         c.Income,
         c.Customer_Segment
     FROM
         Retails c
     LEFT ANTI JOIN
         (SELECT DISTINCT Customer_ID FROM Retails WHERE Year = 2024 AND Month = 'January') jan_orders
     ON c.Customer_ID = jan_orders.Customer_ID
     ORDER BY
         c.Customer_ID ASC
     """)

# Hiển thị kết quả
result.show(50)