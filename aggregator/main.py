from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, to_date

def main():
    spark = SparkSession.builder.appName("TimeSeriesAggregator").getOrCreate()

    input_path = "data/input.csv"
    output_path = "output/"

    # Read input CSV
    df = spark.read.option("header", True).csv(input_path)

    # Convert value to float and timestamp to timestamp type
    df = df.withColumn("value", col("value").cast("float")) \
           .withColumn("timestamp", col("timestamp").cast("timestamp")) \
           .withColumn("date", to_date("timestamp"))

    # Group by metric and time bucket (day) and compute aggregations
    result = df.groupBy("metric", "date").agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value")
    ).orderBy("metric", "date")

    # Write output
    result.write.option("header", True).csv(output_path)

    spark.stop()

if __name__ == "__main__":
    main()

