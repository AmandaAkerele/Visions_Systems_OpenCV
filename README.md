from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Read CSV into Spark DataFrame") \
    .getOrCreate()

# Read CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/path/to/csv/file.csv")

# Show DataFrame
df.show()

# Stop SparkSession
spark.stop()
