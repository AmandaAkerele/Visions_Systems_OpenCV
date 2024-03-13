from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read SAS file") \
    .getOrCreate()

# Define path to SAS file
sas_file_path = sas.saslib('hsp_ext', path=r"L:\Groups\CAD\YHS InDepth\YHS In-Depth 2023 Nov Release\FACILITY_FILES\ED_FACILITY_LIST")

df = pd.read_sas(sas_file_path)

df_spark = spark.createDataFrame(df)

# # Read SAS file into a PySpark DataFrame
# df = spark.read.format(r"L:\Groups\CAD\YHS InDepth\YHS In-Depth 2023 Nov Release\FACILITY_FILES\ED_FACILITY_LIST") \
#     .load(sas_file_path)

# Show DataFrame schema and sample rows
df_spark .printSchema()
df_spark .show(5)

