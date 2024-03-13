
sas.saslib('hsp_ext', path=r"L:\Groups\CAD\YHS InDepth\YHS In-Depth 2023 Nov Release\FACILITY_FILES\ED_FACILITY_LIST")


from pyspark.sql.functions import col
# Construct the file path for the Parquet file
file_path = "/Data/GUD/PROD/NACRS/Data/nacrs_gud_2022_2023.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(file_path)

# List of columns to keep
columns_to_keep = [
    'AM_CARE_KEY', 'SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'AMCARE_GROUP_CODE',
    'FACILITY_AM_CARE_NUM', 'TRIAGE_DATE', 'TRIAGE_TIME', 'DATE_OF_REGISTRATION', 'REGISTRATION_TIME',
    'DISPOSITION_DATE', 'DISPOSITION_TIME', 'VISIT_DISPOSITION', 'WAIT_TIME_TO_PIA_HOURS',
    'LOS_HOURS', 'WAIT_TIME_TO_INPATIENT_HOURS', 'TIME_PHYSICAN_INIT_ASSESSMENT',
    'ED_VISIT_IND_CODE', 'GENDER', 'AGE_NUM'
]

# Selecting the specific columns from the DataFrame
df_nacrs_yr = df.select(columns_to_keep)

# Apply filter conditions
df_nacrs_yr = df_nacrs_yr.filter(
    (col("ED_VISIT_IND_CODE") == "1") & (col("AMCARE_GROUP_CODE") == "ED")
)

# Get the row count
row_count = df_nacrs_yr.count()
print(f"Row count: {row_count}")

# Get the column count
column_count = len(df_nacrs_yr.columns)
print(f"Column count: {column_count}")












































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




