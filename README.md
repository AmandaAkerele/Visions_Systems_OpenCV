from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Covert Code to PySpark") \
    .getOrCreate()

# Set dataset and year variables
dataset = 'NACRS'
year = 2022

# Define column filters
col_filters = ["AM_CARE_KEY", "SUBMISSION_FISCAL_YEAR", "FACILITY_PROVINCE",
               "AMCARE_GROUP_CODE", "FACILITY_AM_CARE_NUM", "TRIAGE_DATE",
               "TRIAGE_TIME", "DATE_OF_REGISTRATION", "REGISTRATION_TIME",
               "DISPOSITION_DATE", "DISPOSITION_TIME", "VISIT_DISPOSITION",
               "WAIT_TIME_TO_PIA_HOURS", "LOS_HOURS", "WAIT_TIME_TO_INPATIENT_HOURS",
               "TIME_PHYSICAN_INIT_ASSESSMENT", "ED_VISIT_IND_CODE", "GENDER", "AGE_NUM"]

# Read Parquet file into a DataFrame
yearly_df = spark.read.parquet('/home/cihiusr/Data/GUD/PROD/{}/{}/Data/parquet/{}_gud_{}_{}.parquet'.format(dataset, year, dataset.lower(), year, year+1),
                                schema=col_filters)

# Show DataFrame
yearly_df.show()

# Stop SparkSession
spark.stop()
