from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read SAS file") \
    .getOrCreate()

# Define path to SAS file
sas_file_path = "path/to/your/sas/file.sas7bdat"

# Read SAS file into a PySpark DataFrame
df = spark.read.format("com.github.saurfang.sas.spark") \
    .load(sas_file_path)

# Show DataFrame schema and sample rows
df.printSchema()
df.show(5)

# Stop SparkSession
spark.stop()














































from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read SAS file") \
    .getOrCreate()

# Specify the path to your SAS file
sas_file_path = "path/to/your/sas/file.sas7bdat"

# Read the SAS file into a DataFrame
sas_df = spark.read.format("sas7bdat").load(sas_file_path)

# Show the DataFrame schema and some sample rows
sas_df.printSchema()
sas_df.show(5)

# Stop SparkSession
spark.stop()

hhh

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read SAS file") \
    .getOrCreate()

# Specify the path to your SAS file
sas_file_path = "path/to/your/sas/file.sas7bdat"

# Read the SAS file into a DataFrame
sas_df = spark.read.format("com.github.saurfang.sas.spark").load(sas_file_path)

# Show the DataFrame schema and some sample rows
sas_df.printSchema()
sas_df.show(5)

# Stop SparkSession
spark.stop()




