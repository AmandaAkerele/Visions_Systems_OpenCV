from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read Excel File") \
    .getOrCreate()

# Define the file path of the Excel file (assuming it's saved as CSV)
file_path = '/path/to/your/file.csv'

# Read the Excel file into a Spark DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the schema and first few rows of the DataFrame
df.printSchema()
df.show()

# Stop the Spark session
spark.stop()









df_fac_a = '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/ED_FACILITY_LIST_FINAL.xlsx'
df_ucc_a ='/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/UCC_FY2022.xlsx'
df_dq_a  ='/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/SUBMITTING_FAC_DQ.xlsx'
df_ps_a ='/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/partial_submitting_fac.xlsx'
df_lookup_a ='/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/fy22_look_up_table.xlsx'



from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read Excel Files") \
    .getOrCreate()

# Define file paths
df_fac_a = '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/ED_FACILITY_LIST_FINAL.xlsx'
df_ucc_a = '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/UCC_FY2022.xlsx'
df_dq_a  = '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/SUBMITTING_FAC_DQ.xlsx'
df_ps_a = '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/partial_submitting_fac.xlsx'
df_lookup_a = '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/FACILITY_FILES/ED_FACILITY_LIST/fy22_look_up_table.xlsx'

# Read Excel files into Spark DataFrames
df_fac = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .option("inferSchema", "true") \
    .load(df_fac_a)

df_ucc = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .option("inferSchema", "true") \
    .load(df_ucc_a)

df_dq = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .option("inferSchema", "true") \
    .load(df_dq_a)

df_ps = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .option("inferSchema", "true") \
    .load(df_ps_a)

df_lookup = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .option("inferSchema", "true") \
    .load(df_lookup_a)

# Show the schema of each DataFrame
df_fac.printSchema()
df_ucc.printSchema()
df_dq.printSchema()
df_ps.printSchema()
df_lookup.printSchema()

# Stop the Spark session
spark.stop()

