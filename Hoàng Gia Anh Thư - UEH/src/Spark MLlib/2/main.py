import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

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

# Lọc dữ liệu vị trí hợp lệ (trong NYC)
df_location = df.filter(
    (col("pickup_longitude").between(-74.3, -73.7)) &
    (col("pickup_latitude").between(40.5, 40.9))
).select("pickup_longitude", "pickup_latitude")

print(f"Số điểm đón khách: {df_location.count()}")

# Lấy mẫu để tăng tốc độ xử lý
df_sample = df_location.sample(0.01, seed=42)  # Lấy 1% dữ liệu
print(f"Số điểm mẫu: {df_sample.count()}")

# Tạo vector features
assembler = VectorAssembler(
    inputCols=["pickup_longitude", "pickup_latitude"],
    outputCol="features"
)

df_features = assembler.transform(df_sample)

# Thử nghiệm các số cụm khác nhau
k_values = [5, 10, 15, 20]
results = []

for k in k_values:
    kmeans = KMeans(k=k, featuresCol="features", seed=42)
    model = kmeans.fit(df_features)
    
    predictions = model.transform(df_features)
    
    # Đánh giá bằng Silhouette Score
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    
    results.append((k, silhouette))
    print(f"K={k}, Silhouette Score: {silhouette:.3f}")

# Chọn K tốt nhất - Sử dụng Python thuần
import builtins
best_k = builtins.max(results, key=lambda x: x[1])[0]
best_score = builtins.max(results, key=lambda x: x[1])[1]

print(f"\nK tốt nhất: {best_k} với Silhouette Score: {best_score:.3f}")

# Huấn luyện mô hình cuối cùng
final_kmeans = KMeans(k=best_k, featuresCol="features", seed=42)
final_model = final_kmeans.fit(df_features)
final_predictions = final_model.transform(df_features)

# Phân tích các cụm
print("\nPhân tích các cụm:")
cluster_stats = final_predictions.groupBy("prediction").agg(
    count("*").alias("count"),
    avg("pickup_longitude").alias("avg_longitude"),
    avg("pickup_latitude").alias("avg_latitude")
).orderBy("prediction")

cluster_stats.show()

# Hiển thị tâm các cụm
print("\nTâm các cụm:")
centers = final_model.clusterCenters()
for i, center in enumerate(centers):
    print(f"Cụm {i}: Longitude={center[0]:.4f}, Latitude={center[1]:.4f}")

spark.stop()