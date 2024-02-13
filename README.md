from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("NACRS Data Processing").getOrCreate()

# Define the options
nacrs_options = {
    'keep': 'AM_CARE_KEY SUBMISSION_FISCAL_YEAR FACILITY_PROVINCE AMCARE_GROUP_CODE FACILITY_AM_CARE_NUM TRIAGE_DATE TRIAGE_TIME DATE_OF_REGISTRATION REGISTRATION_TIME DISPOSITION_DATE DISPOSITION_TIME VISIT_DISPOSITION WAIT_TIME_TO_PIA_HOURS LOS_HOURS WAIT_TIME_TO_INPATIENT_HOURS TIME_PHYSICAN_INIT_ASSESSMENT ED_VISIT_IND_CODE AMCARE_GROUP_CODE GENDER AGE_NUM',
    'where': 'ED_VISIT_IND_CODE in ("1") and AMCARE_GROUP_CODE in ("ED")'
}

nacrs_options2 = {'keep': 'AM_CARE_KEY'}

# Read the ambulatory_care data from a Parquet file
nacrs_yr_df = spark.read.parquet("path_to_ambulatory_care.parquet")

# Filter and select columns as per nacrs_options
selected_columns = nacrs_options['keep'].split()
nacrs_yr_df = nacrs_yr_df.filter((col('ED_VISIT_IND_CODE') == "1") & (col('AMCARE_GROUP_CODE') == "ED")) \
                         .select(*selected_columns)

# Rename columns to uppercase
nacrs_yr_df = nacrs_yr_df.toDF(*[c.upper() for c in nacrs_yr_df.columns])

# Read the nacrs_gud_dups_2022_2023 data from a Parquet file
df_dups_a = spark.read.parquet("path_to_nacrs_gud_dups_2022_2023.parquet")

# Filter and select columns as per nacrs_options2
df_dups_a = df_dups_a.select(*nacrs_options2['keep'].split())

# Show the data (for verification)
nacrs_yr_df.show()
df_dups_a.show()
