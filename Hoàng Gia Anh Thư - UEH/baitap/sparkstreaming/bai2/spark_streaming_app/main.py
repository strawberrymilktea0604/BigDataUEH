import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("sample").getOrCreate()

    # Define schema of the csv
    userSchema = (StructType()
                  .add("name", "string")
                  .add("salary", "integer"))

    # Spark streaming is waiting for csv files to be pushed to "./resources/csv_files/" folder.
    dfCSV = (spark.readStream
             .option("sep", ",")
             .option("header", "false")
             .schema(userSchema)
             .csv("C:/Users/minhk/Downloads/resources/csv_files"))

    # We have defined the total salary per name. Note that this is a streaming DataFrame
    # which represents the running sum of the stream.
    dfCSV.createOrReplaceTempView("salary")
    totalSalary = spark.sql("select name, sum(salary) from salary group by name")

    # Alternative using DataFrame API:
    # totalSalary = dfCSV.groupBy("name").sum("salary")

    # All that is left is to actually start receiving data and computing the counts.
    # To do this, we set it up to print the complete set of counts (specified by outputMode("complete"))
    # to the console every time they are updated. And then start the streaming computation using start().

    # Start running the query that prints the running counts to the console
    query = (totalSalary.writeStream
             .outputMode("complete")
             .format("console")
             .start())

    query.awaitTermination()