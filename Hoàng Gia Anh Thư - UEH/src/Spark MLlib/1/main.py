import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

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
    
# Làm sạch dữ liệu
df_clean = df.filter(
    (col("tip_amount") >= 0) &
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0) &
    (col("payment_type") == 1)
).select("tip_amount", "fare_amount", "trip_distance", "passenger_count")

print(f"Sau khi làm sạch: {df_clean.count()}")

# Thống kê mô tả
print("\nThống kê dữ liệu:")
df_clean.describe().show()

# Tạo features
assembler = VectorAssembler(
    inputCols=["fare_amount", "trip_distance", "passenger_count"],
    outputCol="features"
)

df_features = assembler.transform(df_clean)

# Chia train/test
train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

# Huấn luyện mô hình
lr = LinearRegression(featuresCol="features", labelCol="tip_amount")
model = lr.fit(train_df)

# Dự đoán
predictions = model.transform(test_df)

# Đánh giá
evaluator = RegressionEvaluator(labelCol="tip_amount", predictionCol="prediction")
rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

print(f"\nKết quả mô hình:")
print(f"RMSE: {rmse:.3f}")
print(f"R²: {r2:.3f}")

# Hiển thị một số dự đoán
print("\nMẫu dự đoán:")
predictions.select("tip_amount", "prediction", "fare_amount", "trip_distance").show(10)

spark.stop()