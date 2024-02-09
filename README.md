from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ReadExcelFile") \
    .getOrCreate()

# Define the file path
file_path = '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/ED_FACILITY_LIST_FINAL.xlsx'

# Read the Excel file into a DataFrame df_fac_a
df_fac_a = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \  # If the first row contains column names
    .option("inferSchema", "true") \  # Infer schema automatically
    .load(file_path)

# Show the DataFrame
df_fac_a.show()
