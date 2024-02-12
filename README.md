from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NACRS_ED_Analysis") \
    .getOrCreate()

# Define NACRS options
nacrs_options = {'keep': 'AM_CARE_KEY SUBMISSION_FISCAL_YEAR FACILITY_PROVINCE AMCARE_GROUP_CODE \
                     FACILITY_AM_CARE_NUM TRIAGE_DATE TRIAGE_TIME DATE_OF_REGISTRATION REGISTRATION_TIME\
                     DISPOSITION_DATE DISPOSITION_TIME VISIT_DISPOSITION WAIT_TIME_TO_PIA_HOURS\
                     LOS_HOURS WAIT_TIME_TO_INPATIENT_HOURS TIME_PHYSICAN_INIT_ASSESSMENT\
                     ED_VISIT_IND_CODE AMCARE_GROUP_CODE GENDER AGE_NUM',
                 'where': 'ED_VISIT_IND_CODE in ("1") and AMCARE_GROUP_CODE in ("ED")'}

# Load ambulatory_care table as Spark DataFrame
nacrs_yr = spark.read.format("csv").options(**nacrs_options).load("path_to_file")

# Load other tables as Spark DataFrames
df_dups_a = spark.read.format("csv").options(**nacrs_options2).load("path_to_file")
df_fac_a = spark.read.format("csv").load("path_to_file")
df_ucc_a = spark.read.format("csv").load("path_to_file")
df_dq_a = spark.read.format("csv").load("path_to_file")
df_ps_a = spark.read.format("csv").load("path_to_file")
df_lookup_a = spark.read.format("csv").load("path_to_file")

# Convert to Pandas DataFrame if needed
df_nacrs_yr = nacrs_yr.toPandas()
df_dups = df_dups_a.toPandas()
df_fac = df_fac_a.toPandas()
df_ucc = df_ucc_a.toPandas()
df_dq = df_dq_a.toPandas()
df_ps = df_ps_a.toPandas()
df_lookup = df_lookup_a.toPandas()

# Rename columns to uppercase
df_nacrs_yr.columns = map(str.upper, df_nacrs_yr.columns)
df_dups.columns = map(str.upper, df_dups.columns)
df_fac.columns = map(str.upper, df_fac.columns)
df_ucc.columns = map(str.upper, df_ucc.columns)
df_dq.columns = map(str.upper, df_dq.columns)
df_ps.columns = map(str.upper, df_ps.columns)
df_lookup.columns = map(str.upper, df_lookup.columns)

# Optionally, perform data manipulations and analysis using PySpark APIs
# For example:
# df_nacrs_yr.groupBy("GENDER").count().show()

# Stop Spark session
spark.stop()



andoter 
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NACRS_ED_Analysis") \
    .getOrCreate()

# Define NACRS options
nacrs_options = {'header': True,
                 'inferSchema': True,
                 'sep': ','}

# Load ambulatory_care table as Spark DataFrame
nacrs_yr = spark.read.csv("path_to_ambulatory_care_file.csv", **nacrs_options)

# Load other tables as Spark DataFrames
df_dups_a = spark.read.csv("path_to_dups_file.csv", **nacrs_options)
df_fac_a = spark.read.csv("path_to_facility_list_file.csv", **nacrs_options)
df_ucc_a = spark.read.csv("path_to_ucc_file.csv", **nacrs_options)
df_dq_a = spark.read.csv("path_to_dq_file.csv", **nacrs_options)
df_ps_a = spark.read.csv("path_to_ps_file.csv", **nacrs_options)
df_lookup_a = spark.read.csv("path_to_lookup_table.csv", **nacrs_options)

# Optionally, perform any necessary data manipulations
# For example:
# df_nacrs_yr = nacrs_yr.select([list_of_columns])

# Stop Spark session
spark.stop()

