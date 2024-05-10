

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


using this code above, create same format for the code below in redshift 


# Parameters for the query read - optional parameters commented out

schema = "nacrs_enrich_wide"
table = "nacrs_encounter_wide"
columns = ["AM_CARE_KEY", "SUBMISSION_FISCAL_YEAR", "FACILITY_PROVINCE", "AMCARE_GROUP_CODE",
    "FACILITY_AM_CARE_NUM", "TRIAGE_DATE", "TRIAGE_TIME", "DATE_OF_REGISTRATION", "REGISTRATION_TIME",
    "DISPOSITION_DATE", "DISPOSITION_TIME", "VISIT_DISPOSITION", "WAIT_TIME_TO_PIA_HOURS",
    "LOS_HOURS", "WAIT_TIME_TO_INPATIENT_HOURS", "TIME_PHYSICAN_INIT_ASSESSMENT",
    "ED_VISIT_IND_CODE", "GENDER", "AGE_NUM"]
where = "WHERE fiscal_year = 2022"
# env = "uat"
# key_col = "table_primary_key"
# trim = True
# limit = 5

using the code above, i
