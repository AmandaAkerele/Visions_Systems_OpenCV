from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("NACRS Data Processing").getOrCreate()

# Define the file path for the Parquet file (assuming open_year is defined somewhere in your code)
file_path = f"/path/to/NACRS{open_year-2000}/ambulatory_care"

# Read the Parquet file
df = spark.read.parquet(file_path)

# Columns to select
columns_to_select = [
    'AM_CARE_KEY', 'SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'AMCARE_GROUP_CODE',
    'FACILITY_AM_CARE_NUM', 'TRIAGE_DATE', 'TRIAGE_TIME', 'DATE_OF_REGISTRATION', 'REGISTRATION_TIME',
    'DISPOSITION_DATE', 'DISPOSITION_TIME', 'VISIT_DISPOSITION', 'WAIT_TIME_TO_PIA_HOURS',
    'LOS_HOURS', 'WAIT_TIME_TO_INPATIENT_HOURS', 'TIME_PHYSICAN_INIT_ASSESSMENT',
    'ED_VISIT_IND_CODE', 'AMCARE_GROUP_CODE', 'GENDER', 'AGE_NUM'
]

# Selecting the specific columns
df_nacrs_yr = df.select(columns_to_select)

# Applying the filter condition
df_nacrs_yr = df_nacrs_yr.filter((col("ED_VISIT_IND_CODE") == "1") & (col("AMCARE_GROUP_CODE") == "ED"))

# Renaming columns to uppercase
df_nacrs_yr = df_nacrs_yr.select([col(c).alias(c.upper()) for c in df_nacrs_yr.columns])

# df_nacrs_yr is now the DataFrame with the desired transformations
