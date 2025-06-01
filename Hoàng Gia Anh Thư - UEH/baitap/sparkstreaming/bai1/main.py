import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()

    # Create a DataFrame representing the stream of input lines from the connection to localhost:9999
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9998) \
        .load()

    # Split the lines into words
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    # Generate running word count
    wordCounts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()