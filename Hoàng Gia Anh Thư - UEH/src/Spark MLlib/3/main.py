import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


# Dừng tất cả SparkContext đang chạy
SparkContext.setSystemProperty("spark.executor.memory", "2g")
SparkContext.setSystemProperty("spark.driver.memory", "2g")

spark = SparkSession.builder \
    .appName("TripDataAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


# Đọc dữ liệu từ HDFS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/BigData/yellow_tripdata_2016-01.csv")
    
# Chuẩn bị dữ liệu - chỉ lấy thẻ tín dụng (1) và tiền mặt (2)
df_payment = df.filter(
    (col("payment_type").isin([1, 2])) &
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0)
).select("payment_type", "fare_amount", "trip_distance", "passenger_count", "tip_amount")

# Chuyển payment_type thành binary (1: thẻ tín dụng, 0: tiền mặt)
df_payment = df_payment.withColumn("is_credit_card", 
                                 when(col("payment_type") == 1, 1).otherwise(0))

print(f"Tổng số records: {df_payment.count()}")

# Thống kê phân bố
print("\nPhân bố hình thức thanh toán:")
df_payment.groupBy("is_credit_card").count().show()

# Tạo features
assembler = VectorAssembler(
    inputCols=["fare_amount", "trip_distance", "passenger_count", "tip_amount"],
    outputCol="features"
)

df_features = assembler.transform(df_payment)

# Chia train/test
train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

# Huấn luyện mô hình
lr = LogisticRegression(featuresCol="features", labelCol="is_credit_card")
model = lr.fit(train_df)

# Dự đoán
predictions = model.transform(test_df)

# Đánh giá
# AUC Score
auc_evaluator = BinaryClassificationEvaluator(labelCol="is_credit_card", metricName="areaUnderROC")
auc = auc_evaluator.evaluate(predictions)

# Accuracy
accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="is_credit_card", 
    predictionCol="prediction", 
    metricName="accuracy"
)
accuracy = accuracy_evaluator.evaluate(predictions)

print(f"\nKết quả mô hình:")
print(f"AUC: {auc:.3f}")
print(f"Accuracy: {accuracy:.3f}")

# Phân tích hệ số
print("\nHệ số của mô hình:")
coefficients = model.coefficients.toArray()
feature_names = ["fare_amount", "trip_distance", "passenger_count", "tip_amount"]

for i, (feature, coef) in enumerate(zip(feature_names, coefficients)):
    print(f"{feature}: {coef:.4f}")

# Hiển thị mẫu dự đoán
print("\nMẫu dự đoán:")
predictions.select("is_credit_card", "prediction", "probability", "fare_amount", "tip_amount").show(10)

spark.stop()