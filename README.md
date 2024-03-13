from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read SAS file") \
    .getOrCreate()

# Define path to SAS file
sas_file_path = "L:/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST"

# Read SAS file into a PySpark DataFrame
df_spark = spark.read.format("com.github.saurfang.sas.spark") \
    .load(sas_file_path)

# Show DataFrame schema and sample rows
df_spark.printSchema()
df_spark.show(5)

# Stop SparkSession
spark.stop()
