
# # Read Parquet file
# df = spark.read.parquet("/Data/GUD/PROD/NACRS/Data/nacrs_gud_dups_2022_2023.parquet")
# df.show()

df_spark = spark.read.parquet(file path+ "file_name.parquet.gz")\
.select([list of columns])

nacrs_options={'keep':'AM_CARE_KEY SUBMISSION_FISCAL_YEAR FACILITY_PROVINCE AMCARE_GROUP_CODE \
                     FACILITY_AM_CARE_NUM TRIAGE_DATE TRIAGE_TIME DATE_OF_REGISTRATION REGISTRATION_TIME\
                     DISPOSITION_DATE DISPOSITION_TIME VISIT_DISPOSITION WAIT_TIME_TO_PIA_HOURS\
                     LOS_HOURS WAIT_TIME_TO_INPATIENT_HOURS TIME_PHYSICAN_INIT_ASSESSMENT\
                     ED_VISIT_IND_CODE AMCARE_GROUP_CODE GENDER AGE_NUM',
              'where': 'ED_VISIT_IND_CODE in ("1") and AMCARE_GROUP_CODE in ("ED")'}      



CODE PATH 

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ReadParquetWithColumns") \
    .getOrCreate()

# Define file path and file name
file_path = "/Data/GUD/PROD/NACRS/Data/"
file_name = "nacrs_gud_dups_2022_2023.parquet"

# Define list of columns to select
columns_to_select = [
    "AM_CARE_KEY", "SUBMISSION_FISCAL_YEAR", "FACILITY_PROVINCE",
    "AMCARE_GROUP_CODE", "FACILITY_AM_CARE_NUM", "TRIAGE_DATE",
    "TRIAGE_TIME", "DATE_OF_REGISTRATION", "REGISTRATION_TIME",
    "DISPOSITION_DATE", "DISPOSITION_TIME", "VISIT_DISPOSITION",
    "WAIT_TIME_TO_PIA_HOURS", "LOS_HOURS", "WAIT_TIME_TO_INPATIENT_HOURS",
    "TIME_PHYSICAN_INIT_ASSESSMENT", "ED_VISIT_IND_CODE", "GENDER", "AGE_NUM"
]

# Read Parquet file and select specific columns
df_spark = spark.read.parquet(file_path + file_name).select(columns_to_select)

# Filter DataFrame based on conditions
df_filtered = df_spark.filter((df_spark["ED_VISIT_IND_CODE"] == "1") & (df_spark["AMCARE_GROUP_CODE"] == "ED"))

# Show the DataFrame
df_filtered.show()
