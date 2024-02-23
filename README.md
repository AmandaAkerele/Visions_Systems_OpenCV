
Code




Python 3 (ipykernel)
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import warnings
warnings.filterwarnings("ignore")
import pyspark
from pyspark.sql import functions as F
def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape
# import os
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark import SparkContext, SparkConf
# # Setup jar file library paths
# extraClassPath=None
# for file in os.listdir("/opt/jars"):
#     # check only text files
#     if file.endswith('.jar'):
#         if extraClassPath is None:
#             extraClassPath = f"/opt/jars/{file}"
#         else:
#             extraClassPath += f",/opt/jars/{file}"
# # Connect to Spark cluster.
# # REPLACE "SPARK_TOKEN_HERE" WITH A VALID CURRENT SPARK CLUSTER TOKEN
# # REPLACE "USERNAME_HERE" WITH YOUR CIHI USERNAME
# spark = SparkSession.builder \
#                      .master('spark://spkm-aakerele:7077') \
#                      .appName('SparkApp') \
#                      .config("spark.authenticate", "true") \
#                      .config("spark.authenticate.secret", "D1tlsoCjLWJqJi6yt78dmbSv8WxgEcFop0me4sSJb7I=") \
#                      .config("spark.jars", extraClassPath) \
#                      .config("spark.driver.extraClassPath", extraClassPath) \
#                      .getOrCreate()



# create spark session
spark_token = "3bUkSOCLcldTxsSmGyJPOfQEFEbZ2F0kawPuCGGDyl8="

#### initialize spark session
extraClassPath = None
for file in os.listdir("/opt/jars"):
    # check only text files
    if file.endswith('.jar'):
        if extraClassPath is None:
            extraClassPath = f"/opt/jars/{file}"
        else:
            extraClassPath += f",/opt/jars/{file}"
            
# connect to spark cluster
spark = (SparkSession.builder
                     .master(f"spark://spkm-aakerele:7077")
                     .appName("OracleSparkApp")
                     .config("spark.authenticate", "true")
                     .config("spark.authenticate.secret", spark_token)
                     .config("spark.driver.memory", "64g")
                     .config("spark.executor.memory", "64g")
                     .config('spark.executor.cores', '4')
                     .config("spark.jars", extraClassPath)
                     .config("spark.driver.extraClassPath", extraClassPath)
                     .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
                     .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                     .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                     .config("spark.sql.debug.maxToStringFields", 1000)
                     .config('spark.sql.timestampType', "TIMESTAMP_NTZ")
                     .getOrCreate())


24/02/23 11:29:33 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
# # Read sas7bdat file
# df = spark.read.format("com.github.saurfang.sas.spark").load("YOUR_DATA_PATH", forceLowercaseNames=True, inferLong=True)
# df.createOrReplaceTempView("VIEW_NAME")
# df.show()
# # Read Parquet file
# df = spark.read.parquet("/Data/GUD/PROD/NACRS/Data/nacrs_gud_dups_2022_2023.parquet")
# df.show()
# # Construct the file path for the Parquet file
# file_path = "/Data/GUD/PROD/NACRS/Data/nacrs_gud_2022_2023.parquet"

# # Read the Parquet file into a DataFrame
# df = spark.read.parquet(file_path)

# # List of columns to keep, based on the 'keep' option in the SAS code
# columns_to_keep = [
#     'AM_CARE_KEY', 'SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'AMCARE_GROUP_CODE',
#     'FACILITY_AM_CARE_NUM', 'TRIAGE_DATE', 'TRIAGE_TIME', 'DATE_OF_REGISTRATION', 'REGISTRATION_TIME',
#     'DISPOSITION_DATE', 'DISPOSITION_TIME', 'VISIT_DISPOSITION', 'WAIT_TIME_TO_PIA_HOURS',
#     'LOS_HOURS', 'WAIT_TIME_TO_INPATIENT_HOURS', 'TIME_PHYSICAN_INIT_ASSESSMENT',
#     'ED_VISIT_IND_CODE', 'GENDER', 'AGE_NUM'
# ]

# # Selecting the specific columns from the DataFrame
# df_nacrs_yr = df.select(columns_to_keep)

# # Apply filter conditions based on the 'where' clause in the SAS code
# df_nacrs_yr = df_nacrs_yr.filter(
#     (col("ED_VISIT_IND_CODE") == "1") & (col("AMCARE_GROUP_CODE") == "ED")
# )
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
[Stage 1:======================================>                   (8 + 4) / 12]
Row count: 15129442
Column count: 19
                                                                                
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, trim, upper

# # Initialize Spark session
# spark = SparkSession.builder.appName("NACRS Data Processing").getOrCreate()

# # Construct the file path for the Parquet file
# file_path = "/Data/GUD/PROD/NACRS/Data/nacrs_gud_2022_2023.parquet"

# # Read the Parquet file into a DataFrame
# df = spark.read.parquet(file_path)

# # Selecting the specific columns from the DataFrame
# columns_to_keep = [
#     'AM_CARE_KEY', 'SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'AMCARE_GROUP_CODE',
#     'FACILITY_AM_CARE_NUM', 'TRIAGE_DATE', 'TRIAGE_TIME', 'DATE_OF_REGISTRATION', 'REGISTRATION_TIME',
#     'DISPOSITION_DATE', 'DISPOSITION_TIME', 'VISIT_DISPOSITION', 'WAIT_TIME_TO_PIA_HOURS',
#     'LOS_HOURS', 'WAIT_TIME_TO_INPATIENT_HOURS', 'TIME_PHYSICAN_INIT_ASSESSMENT',
#     'ED_VISIT_IND_CODE', 'GENDER', 'AGE_NUM'
# ]
# df_nacrs_yr = df.select(columns_to_keep)

# # Apply filter conditions with data type consideration and trimming
# df_nacrs_yr = df_nacrs_yr.filter(
#     (trim(upper(col("ED_VISIT_IND_CODE"))) == "1") & 
#     (trim(upper(col("AMCARE_GROUP_CODE"))) == "ED")
# )

# # Get the row count
# row_count = df_nacrs_yr.count()
# print(f"Row count: {row_count}")

# # Get the column count
# column_count = len(df_nacrs_yr.columns)
# print(f"Column count: {column_count}")


# Define the file path for the Parquet file
file_path = "/Data/GUD/PROD/NACRS/Data/nacrs_gud_2022_2023.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(file_path)

# Filter the DataFrame where 'ED_VISIT_IND_CODE' is '1'
filtered_df = df.filter(col("ED_VISIT_IND_CODE") == "1")

# Count the number of occurrences
count_occurrences = filtered_df.count()

print(f"Number of occurrences where 'ED_VISIT_IND_CODE' is '1': {count_occurrences}")


Number of occurrences where 'ED_VISIT_IND_CODE' is '1': 15129442
# Define the file path for the Parquet file
file_path = "/Data/GUD/PROD/NACRS/Data/nacrs_gud_2022_2023.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(file_path)

# Filter the DataFrame where 'AMCARE_GROUP_CODE' is 'ED'
filtered_df = df.filter(col("AMCARE_GROUP_CODE") == "ED")

# Count the number of occurrences
count_occurrences = filtered_df.count()

print(f"Number of occurrences where 'AMCARE_GROUP_CODE' is 'ED': {count_occurrences}")
Number of occurrences where 'AMCARE_GROUP_CODE' is 'ED': 15186190
df_nacrs_yr.shape()
(15129442, 19)
pandas_df_nacrs= df_nacrs_yr.limit(10).toPandas()
display(pandas_df_nacrs)
df_nacrs_yr.shape()
# # Define file path and file name
# file_path = "/Data/GUD/PROD/NACRS/Data/"
# file_name = "nacrs_gud_2022_2023.parquet"


# # Define list of columns to select
# columns_to_select = [
#     "AM_CARE_KEY", "SUBMISSION_FISCAL_YEAR", "FACILITY_PROVINCE",
#     "AMCARE_GROUP_CODE", "FACILITY_AM_CARE_NUM", "TRIAGE_DATE",
#     "TRIAGE_TIME", "DATE_OF_REGISTRATION", "REGISTRATION_TIME",
#     "DISPOSITION_DATE", "DISPOSITION_TIME", "VISIT_DISPOSITION",
#     "WAIT_TIME_TO_PIA_HOURS", "LOS_HOURS", "WAIT_TIME_TO_INPATIENT_HOURS",
#     "TIME_PHYSICAN_INIT_ASSESSMENT", "ED_VISIT_IND_CODE", "GENDER", "AGE_NUM"
# ]

# # Read Parquet file and select specific columns
# df_spark = spark.read.parquet(file_path + file_name).select(columns_to_select)
                     
# # Filter DataFrame based on conditions
# df_nacrs_yr = df_spark.filter((df_spark["ED_VISIT_IND_CODE"] == "1") & (df_spark["AMCARE_GROUP_CODE"] == "ED"))

# # Show the DataFrame
# # df_filtered.show()                                                 
# df_nacrs_yr.shape()
# # Load other tables as Spark DataFrames

# df_fac=spark.read.option("header","true").csv("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/ed_facility_list_final.csv")
# df_ucc=spark.read.option("header","true").csv("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/ucc_2022.csv")
# df_dq=spark.read.option("header","true").csv("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/submitting_fac_dq.csv")
# df_ps=spark.read.option("header","true").csv("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/partial_submitting_fac.csv")
# df_ucc=spark.read.option("header","true").csv("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/ed_facility_list_final.csv")
# df_lookup=spark.read.option("header","true").csv("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/fy22_look_up_table.csv")


# # df.show(10,False)

# # # Convert to Pandas DataFrame if needed
# # # df_nacrs_yr = nacrs_yr.toPandas()
# # # # df_dups = df_dups_a.toPandas()
# # df_fac = df_fac_a.toPandas()
# # df_ucc = df_ucc_a.toPandas()
# # df_dq = df_dq_a.toPandas()
# # df_ps = df_ps_a.toPandas()
# # df_lookup = df_lookup_a.toPandas()

# # # Rename columns to uppercase
# # # df_nacrs_yr.columns = map(str.upper, df_nacrs_yr.columns)
# # # df_dups.columns = map(str.upper, df_dups.columns)
# # df_fac.columns = map(str.upper, df_fac.columns)
# # df_ucc.columns = map(str.upper, df_ucc.columns)
# # df_dq.columns = map(str.upper, df_dq.columns)
# # df_ps.columns = map(str.upper, df_ps.columns)
# # df_lookup.columns = map(str.upper, df_lookup.columns)

# # ## perform data manipulations and analysis using PySpark APIs
 
# # # df_nacrs_yr.groupBy("GENDER").count().show()
# Load other tables as Spark DataFrames

# Read CSV file into a DataFrame
df_fac = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/ed_facility_list_final.csv")

df_ucc = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/ucc_2022.csv")

df_dq = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/submitting_fac_dq.csv")

df_ps= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/partial_submitting_fac.csv")

df_lookup= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/fy22_look_up_table.csv")


# Show DataFrame
df_fac.show(2)
df_ucc.show(2)
df_dq.show(2)
df_ps.show(2)
df_lookup.show(2)

# Show the schema of each DataFrame
df_fac.printSchema()
df_ucc.printSchema()
df_dq.printSchema()
df_ps.printSchema()
df_lookup.printSchema()
                                                                                
+----------------------+--------------------+-------+--------------------+---------+-------+--------------------+---------+---------+-----------+-----------+-------------+-------------+-----------+-------+------------+--------------+-------------+-------------+-------------+
|SUBMISSION_FISCAL_YEAR|FACILITY_AM_CARE_NUM|SITE_ID|           SITE_NAME|SITE_PEER|CORP_ID|           CORP_NAME|CORP_PEER|REGION_ID|REGION_NAME|PROVINCE_ID|PROVINCE_NAME| MUNICIPALITY|POSTAL_CODE|INSTNUM|NACRS_ED_FLG|CLOSED_FLAG_ED|FIRST_YEAR_ED|NEW_REGION_ID|REGION_E_DESC|
+----------------------+--------------------+-------+--------------------+---------+-------+--------------------+---------+---------+-----------+-----------+-------------+-------------+-----------+-------+------------+--------------+-------------+-------------+-------------+
|                  2022|               17001|  122.0|Queen Elizabeth H...|       H1|  122.0|Queen Elizabeth H...|       H1|  20034.0| Health PEI|      100.0|           PE|Charlottetown|     C1A8T5|  10001|         0.0|           0.0|         NULL|      20034.0|   Health PEI|
|                  2022|               17003|  120.0|Prince County Hos...|       H2|  120.0|Prince County Hos...|       H2|  20034.0| Health PEI|      100.0|           PE|   Summerside|     C1N2A9|  10003|         0.0|           0.0|         NULL|      20034.0|   Health PEI|
+----------------------+--------------------+-------+--------------------+---------+-------+--------------------+---------+---------+-----------+-----------+-------------+-------------+-----------+-------+------------+--------------+-------------+-------------+-------------+
only showing top 2 rows

+----------------------+--------------------+---------+-----------+-------+-------+--------------------+---------+-------+--------------------+---------+------------+-------+---------+
|SUBMISSION_FISCAL_YEAR|FACILITY_AM_CARE_NUM|NUMERATOR|DENOMINATOR|UCC_PCT|SITE_ID|           SITE_NAME|SITE_PEER|CORP_ID|           CORP_NAME|REGION_ID| REGION_NAME|PROV_ID|PROV_NAME|
+----------------------+--------------------+---------+-----------+-------+-------+--------------------+---------+-------+--------------------+---------+------------+-------+---------+
|                  2022|               88142|   6791.0|     6791.0|    1.0|20390.0|La Crete Health C...|     NULL|20390.0|La Crete Health C...|  20211.0|  North Zone|20018.0|  Alberta|
|                  2022|               88155|  46885.0|    46885.0|    1.0|99724.0|South Calgary Hea...|     NULL|99724.0|South Calgary Hea...|  20208.0|Calgary Zone|20018.0|  Alberta|
+----------------------+--------------------+---------+-----------+-------+-------+--------------------+---------+-------+--------------------+---------+------------+-------+---------+
only showing top 2 rows

+-----------+--------------------+--------------------+-------+-------+---------+--------------------+--------+----+----+
|FISCAL_YEAR|FACILITY_AM_CARE_NUM|       FACILITY_NAME|SITE_ID|CORP_ID|REGION_ID|         REGION_NAME|PROVINCE|PEER| IND|
+-----------+--------------------+--------------------+-------+-------+---------+--------------------+--------+----+----+
|     2012.0|             98201.0|Royal Jubilee Hos...|10101.0|10101.0|   9057.0|Vancouver Island ...|      BC|   T|ELOS|
|     2012.0|             98202.0|Victoria General ...| 9165.0| 9165.0|   9057.0|Vancouver Island ...|      BC|   T|ELOS|
+-----------+--------------------+--------------------+-------+-------+---------+--------------------+--------+----+----+
only showing top 2 rows

+-----------+--------------------+-------+-------+--------------------+
|FISCAL_YEAR|FACILITY_AM_CARE_NUM|SITE_ID|CORP_ID|       FACILITY_NAME|
+-----------+--------------------+-------+-------+--------------------+
|       2009|               53974| 5074.0| 5074.0|Wilson Memorial G...|
|       2009|               68003|  709.0|  709.0|      Grace Hospital|
+-----------+--------------------+-------+-------+--------------------+
only showing top 2 rows

+---------------+--------------------+--------------------+-------+--------------+----------+----------------+------------+---------+--------------+-----------+--------+-------------+-----------+----------------------+------------------+----------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+
|ORGANIZATION_ID|       HOSPITAL_NAME|           SITE_NAME|INSTNUM|ORIGINAL_ID_OI|SITE_ID_OI|HICS_HOSPITAL_ID|HICS_SITE_ID|REGION_ID|   REGION_NAME|CLOSED_FLAG|END_YEAR|PROVINCE_CODE|PROVINCE_ID|HOSPITAL_PEER_GROUP_V3|SITE_PEER_GROUP_V3|FIRST_YEAR|NEW_FY13_ORG_ID|NEW_FY14_ORG_ID|NEW_FY15_ORG_ID|NEW_FY16_ORG_ID|NEW_FY17_ORG_ID|NEW_FY18_ORG_ID|NEW_FY19_ORG_ID|NEW_FY20_ORG_ID|NEW_FY21_ORG_ID|NEW_FY22_ORG_ID|
+---------------+--------------------+--------------------+-------+--------------+----------+----------------+------------+---------+--------------+-----------+--------+-------------+-----------+----------------------+------------------+----------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+
|           10.0|Bonavista Peninsu...|Bonavista Peninsu...|  00007|          10.0|      10.0|            10.0|        10.0|  10147.0|Eastern Health|        0.0|    NULL|           NL|    10151.0|                    H3|                H3|      NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|
|           11.0|Bonne Bay Health ...|Bonne Bay Health ...|  00008|          11.0|      11.0|            11.0|        11.0|  10149.0|Western Health|        0.0|    NULL|           NL|    10151.0|                    H3|                H3|      NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|           NULL|
+---------------+--------------------+--------------------+-------+--------------+----------+----------------+------------+---------+--------------+-----------+--------+-------------+-----------+----------------------+------------------+----------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+
only showing top 2 rows

root
 |-- SUBMISSION_FISCAL_YEAR: integer (nullable = true)
 |-- FACILITY_AM_CARE_NUM: string (nullable = true)
 |-- SITE_ID: double (nullable = true)
 |-- SITE_NAME: string (nullable = true)
 |-- SITE_PEER: string (nullable = true)
 |-- CORP_ID: double (nullable = true)
 |-- CORP_NAME: string (nullable = true)
 |-- CORP_PEER: string (nullable = true)
 |-- REGION_ID: double (nullable = true)
 |-- REGION_NAME: string (nullable = true)
 |-- PROVINCE_ID: double (nullable = true)
 |-- PROVINCE_NAME: string (nullable = true)
 |-- MUNICIPALITY: string (nullable = true)
 |-- POSTAL_CODE: string (nullable = true)
 |-- INSTNUM: string (nullable = true)
 |-- NACRS_ED_FLG: double (nullable = true)
 |-- CLOSED_FLAG_ED: double (nullable = true)
 |-- FIRST_YEAR_ED: string (nullable = true)
 |-- NEW_REGION_ID: double (nullable = true)
 |-- REGION_E_DESC: string (nullable = true)

root
 |-- SUBMISSION_FISCAL_YEAR: integer (nullable = true)
 |-- FACILITY_AM_CARE_NUM: integer (nullable = true)
 |-- NUMERATOR: double (nullable = true)
 |-- DENOMINATOR: double (nullable = true)
 |-- UCC_PCT: double (nullable = true)
 |-- SITE_ID: double (nullable = true)
 |-- SITE_NAME: string (nullable = true)
 |-- SITE_PEER: string (nullable = true)
 |-- CORP_ID: double (nullable = true)
 |-- CORP_NAME: string (nullable = true)
 |-- REGION_ID: double (nullable = true)
 |-- REGION_NAME: string (nullable = true)
 |-- PROV_ID: double (nullable = true)
 |-- PROV_NAME: string (nullable = true)

root
 |-- FISCAL_YEAR: double (nullable = true)
 |-- FACILITY_AM_CARE_NUM: double (nullable = true)
 |-- FACILITY_NAME: string (nullable = true)
 |-- SITE_ID: double (nullable = true)
 |-- CORP_ID: double (nullable = true)
 |-- REGION_ID: double (nullable = true)
 |-- REGION_NAME: string (nullable = true)
 |-- PROVINCE: string (nullable = true)
 |-- PEER: string (nullable = true)
 |-- IND: string (nullable = true)

root
 |-- FISCAL_YEAR: integer (nullable = true)
 |-- FACILITY_AM_CARE_NUM: integer (nullable = true)
 |-- SITE_ID: double (nullable = true)
 |-- CORP_ID: double (nullable = true)
 |-- FACILITY_NAME: string (nullable = true)

root
 |-- ORGANIZATION_ID: double (nullable = true)
 |-- HOSPITAL_NAME: string (nullable = true)
 |-- SITE_NAME: string (nullable = true)
 |-- INSTNUM: string (nullable = true)
 |-- ORIGINAL_ID_OI: double (nullable = true)
 |-- SITE_ID_OI: double (nullable = true)
 |-- HICS_HOSPITAL_ID: double (nullable = true)
 |-- HICS_SITE_ID: double (nullable = true)
 |-- REGION_ID: double (nullable = true)
 |-- REGION_NAME: string (nullable = true)
 |-- CLOSED_FLAG: double (nullable = true)
 |-- END_YEAR: string (nullable = true)
 |-- PROVINCE_CODE: string (nullable = true)
 |-- PROVINCE_ID: double (nullable = true)
 |-- HOSPITAL_PEER_GROUP_V3: string (nullable = true)
 |-- SITE_PEER_GROUP_V3: string (nullable = true)
 |-- FIRST_YEAR: string (nullable = true)
 |-- NEW_FY13_ORG_ID: double (nullable = true)
 |-- NEW_FY14_ORG_ID: double (nullable = true)
 |-- NEW_FY15_ORG_ID: double (nullable = true)
 |-- NEW_FY16_ORG_ID: string (nullable = true)
 |-- NEW_FY17_ORG_ID: double (nullable = true)
 |-- NEW_FY18_ORG_ID: string (nullable = true)
 |-- NEW_FY19_ORG_ID: string (nullable = true)
 |-- NEW_FY20_ORG_ID: double (nullable = true)
 |-- NEW_FY21_ORG_ID: double (nullable = true)
 |-- NEW_FY22_ORG_ID: double (nullable = true)

df_fac.shape()
(507, 20)
df_ucc.shape()
(22, 14)
df_dq.shape()
(10, 10)
df_ps.shape()
(91, 5)
df_lookup.shape()
(770, 27)
# # Alias both DataFrames
# df_fac_alias = df_fac.alias("fac")
# df_dq_alias = df_dq.alias("dq")

# # Perform the inner join using aliases
# tmp_ed_facility_org_a = df_fac_alias.join(
#     df_dq_alias,
#     (col("fac.FACILITY_AM_CARE_NUM") == col("dq.FACILITY_AM_CARE_NUM")) &
#     (col("fac.SUBMISSION_FISCAL_YEAR") == col("dq.FISCAL_YEAR")),
#     'inner'
# )

# # Select columns and handle duplicates
# # List all columns from df_fac and selectively from df_dq to avoid duplicates
# selected_columns = [col(f"fac.{column_name}") for column_name in df_fac.columns]

# # Add columns from df_dq, renaming those that conflict
# conflicting_columns = ['SITE_ID', 'FACILITY_AM_CARE_NUM', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 'REGION_NAME']  # Adjust based on your data for future use
# for column_name in df_dq.columns:
#     if column_name not in conflicting_columns:
#         selected_columns.append(col(f"dq.{column_name}"))

# # Construct the final DataFrame with selected columns
# tmp_ed_facility_org_a = tmp_ed_facility_org_a.select(selected_columns)

# tmp_ed_facility_org_a.printSchema()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit
# from pyspark.sql.types import StringType

# # Create a SparkSession
# spark = SparkSession.builder \
#     .appName("Merge and Filter DataFrames") \
#     .getOrCreate()

# # Assuming you have already loaded your DataFrames df_fac, df_dq, and df_ps into Spark DataFrames

# # Merge dataframes on specified columns and suffixes
# merged_df = df_fac.join(df_dq,
#                         (df_fac['FACILITY_AM_CARE_NUM'] == df_dq['FACILITY_AM_CARE_NUM']) &
#                         (df_fac['SUBMISSION_FISCAL_YEAR'] == df_dq['FISCAL_YEAR']),
#                         how='inner') \
#                   .withColumnRenamed('FACILITY_AM_CARE_NUM', 'FACILITY_AM_CARE_NUM_df_fac') \
#                   .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'SUBMISSION_FISCAL_YEAR_df_fac') \
#                   .withColumnRenamed('FISCAL_YEAR', 'FISCAL_YEAR_df_dq')

# # Define a list of columns to keep
# columns_to_keep = [
#     'FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'CORP_ID',
#     'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG'
# ]

# # Add 'TYPE' column with values 'DQ' for merged_df
# merged_df = merged_df.withColumn('TYPE', lit('DQ'))

# # Create DataFrames t3 and t4 based on conditions
# t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1) \
#           .select(*columns_to_keep) \
#           .withColumn('TYPE', lit('SL')) \
#           .withColumn('IND', lit(''))

# ps = df_ps.filter(df_ps['FISCAL_YEAR'].cast(StringType()) == '2022')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Merge and Filter DataFrames") \
    .getOrCreate()

# Alias both DataFrames
df_fac_alias = df_fac.alias("fac")
df_dq_alias = df_dq.alias("dq")

# Perform the inner join using aliases
merged_df = df_fac_alias.join(
    df_dq_alias,
    (col("fac.FACILITY_AM_CARE_NUM") == col("dq.FACILITY_AM_CARE_NUM")) &
    (col("fac.SUBMISSION_FISCAL_YEAR") == col("dq.FISCAL_YEAR")),
    'inner'
)

# Select columns and handle duplicates
# List all columns from df_fac and selectively from df_dq to avoid duplicates
selected_columns = [col(f"fac.{column_name}") for column_name in df_fac.columns]

# Add columns from df_dq, renaming those that conflict
conflicting_columns = ['SITE_ID', 'FACILITY_AM_CARE_NUM', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 'REGION_NAME']  # Adjust based on your data
for column_name in df_dq.columns:
    if column_name not in conflicting_columns:
        selected_columns.append(col(f"dq.{column_name}"))

# Construct the final DataFrame with selected columns
merged_df = merged_df.select(selected_columns)

# Define a list of columns to keep
columns_to_keep = [
    'FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'CORP_ID',
    'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG'
]

# Add 'TYPE' column with values 'DQ' for merged_df
merged_df = merged_df.withColumn('TYPE', lit('DQ'))




# Create DataFrames t3 and t4 based on conditions
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1) \
          .select(*columns_to_keep) \
          .withColumn('TYPE', lit('SL')) \
          .withColumn('IND', lit(''))

# Handle conflicting column names for t4
selected_columns_t4 = [col(f"fac.{column_name}") for column_name in columns_to_keep]

# Add columns from df_ps, renaming those that conflict
conflicting_columns_t4 = ['FACILITY_AM_CARE_NUM']  # Adjust based on your data
for column_name in df_ps.columns:
    if column_name not in conflicting_columns_t4:
        selected_columns_t4.append(col(f"ps.{column_name}"))
ps = df_ps.filter(df_ps['FISCAL_YEAR'] == 2022)
t4 = df_fac.join(ps, df_fac['FACILITY_AM_CARE_NUM'] == ps['FACILITY_AM_CARE_NUM'], 'inner')
t4 = t4.withColumn('TYPE', F.lit('PS')).withColumn('IND', F.lit(''))

from pyspark.sql.functions import col, lit

# Assuming df_fac, df_ps are already defined DataFrames

# Filter df_ps for the year 2022
ps = df_ps.filter(df_ps['FISCAL_YEAR'] == 2022)

# List of columns from df_fac to include
columns_to_keep = [col(f"fac.{column_name}") for column_name in df_fac.columns]

# Handle conflicting columns for t4
# 'CORP_ID' is the conflicting column to be handled
conflicting_columns_t4 = ['CORP_ID', 'FACILITY_AM_CARE_NUM','SITE_ID']

# Add columns from df_ps, excluding or renaming conflicting ones
for column_name in df_ps.columns:
    if column_name not in conflicting_columns_t4:
        columns_to_keep.append(col(f"ps.{column_name}"))
    else:
        # If the column is conflicting, rename it (e.g., append '_ps')
        columns_to_keep.append(col(f"ps.{column_name}").alias(f"{column_name}_ps"))

# Create t4 DataFrame with selected columns
t4 = df_fac.alias("fac").join(ps.alias("ps"), col("fac.FACILITY_AM_CARE_NUM") == col("ps.FACILITY_AM_CARE_NUM"), 'inner') \
          .select(columns_to_keep) \
          .withColumn('TYPE', lit('PS')) \
          .withColumn('IND', lit(''))



# Using unionByName with allowMissingColumns=True for combining DataFrames
tmp_ed_facility_org = merged_df.unionByName(t3, allowMissingColumns=True) \
                              .unionByName(t4, allowMissingColumns=True) \
                              .distinct()

filtered_ed_fac = df_fac.join(tmp_ed_facility_org.select('CORP_ID').distinct(), 'CORP_ID')
tmp_cnt_ed_facility_org = filtered_ed_fac.groupBy('CORP_ID').count().withColumnRenamed('count', 'CORP_CNT')


ed_facility_org = tmp_ed_facility_org.join(tmp_cnt_ed_facility_org, 'CORP_ID')
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Specify the columns in the desired order
columns_to_keep = [
    'SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM', 'SITE_ID', 'CORP_ID', 
    'REGION_ID', 'PROVINCE_ID', 'TYPE', 'IND', 'NACRS_ED_FLG', 'CORP_CNT'
]

# Select the desired columns from the PySpark DataFrame
ed_facility_org_spark = ed_facility_org.select(*columns_to_keep)

# Display the resulting DataFrame
ed_facility_org_spark.show()

+----------------------+--------------------+-------+-------+---------+-----------+----+---+------------+--------+
|SUBMISSION_FISCAL_YEAR|FACILITY_AM_CARE_NUM|SITE_ID|CORP_ID|REGION_ID|PROVINCE_ID|TYPE|IND|NACRS_ED_FLG|CORP_CNT|
+----------------------+--------------------+-------+-------+---------+-----------+----+---+------------+--------+
|                  2022|               54250| 5060.0| 5060.0|   5013.0|     5001.0|  PS|   |         0.0|       1|
|                  2022|               54267| 5119.0| 5119.0|   5011.0|     5001.0|  PS|   |         0.0|       1|
|                  2022|               55538| 5065.0|81118.0|   5013.0|     5001.0|  PS|   |         0.0|       2|
|                  2022|               88030|  956.0|  956.0|  20209.0|    20018.0|  PS|   |         0.0|       1|
|                  2022|               29061|99012.0|99012.0|  80289.0|     2000.0|  SL|   |         1.0|       1|
|                  2022|               48006|60309.0|80335.0|  80317.0|     4000.0|  SL|   |         1.0|       8|
|                  2022|               48008|61049.0|80335.0|  80317.0|     4000.0|  SL|   |         1.0|       8|
|                  2022|               48015|61238.0|80337.0|  80319.0|     4000.0|  SL|   |         1.0|       7|
|                  2022|               48022|61313.0|80337.0|  80319.0|     4000.0|  SL|   |         1.0|       7|
|                  2022|               48023|61229.0|80337.0|  80319.0|     4000.0|  SL|   |         1.0|       7|
|                  2022|               48024|61280.0|80337.0|  80319.0|     4000.0|  SL|   |         1.0|       7|
|                  2022|               48029|60434.0|80345.0|  80323.0|     4000.0|  SL|   |         1.0|       8|
|                  2022|               48032|60901.0|80338.0|  80320.0|     4000.0|  SL|   |         1.0|       8|
|                  2022|               48037|62617.0|80339.0|  80321.0|     4000.0|  SL|   |         1.0|       8|
|                  2022|               48039|61241.0|80340.0|  80322.0|     4000.0|  SL|   |         1.0|       3|
|                  2022|               48044|61408.0|80344.0|  80322.0|     4000.0|  SL|   |         1.0|       4|
|                  2022|               48053|61231.0|80341.0|  80322.0|     4000.0|  SL|   |         1.0|       4|
|                  2022|               48063|60258.0|80345.0|  80323.0|     4000.0|  SL|   |         1.0|       8|
|                  2022|               48076|60496.0|80347.0|  80325.0|     4000.0|  SL|   |         1.0|       8|
|                  2022|               48083|60306.0|80348.0|  80327.0|     4000.0|  SL|   |         1.0|       7|
+----------------------+--------------------+-------+-------+---------+-----------+----+---+------------+--------+
only showing top 20 rows

ed_facility_org_spark .shape()
(42, 10)
ed_facility_org_spark_pd  = ed_facility_org_spark.toPandas()
display(ed_facility_org_spark_pd)
SUBMISSION_FISCAL_YEAR	FACILITY_AM_CARE_NUM	SITE_ID	CORP_ID	REGION_ID	PROVINCE_ID	TYPE	IND	NACRS_ED_FLG	CORP_CNT
0	2022	54250	5060.0	5060.0	5013.0	5001.0	PS		0.0	1
1	2022	54267	5119.0	5119.0	5011.0	5001.0	PS		0.0	1
2	2022	55538	5065.0	81118.0	5013.0	5001.0	PS		0.0	2
3	2022	88030	956.0	956.0	20209.0	20018.0	PS		0.0	1
4	2022	29061	99012.0	99012.0	80289.0	2000.0	SL		1.0	1
5	2022	48006	60309.0	80335.0	80317.0	4000.0	SL		1.0	8
6	2022	48008	61049.0	80335.0	80317.0	4000.0	SL		1.0	8
7	2022	48015	61238.0	80337.0	80319.0	4000.0	SL		1.0	7
8	2022	48022	61313.0	80337.0	80319.0	4000.0	SL		1.0	7
9	2022	48023	61229.0	80337.0	80319.0	4000.0	SL		1.0	7
10	2022	48024	61280.0	80337.0	80319.0	4000.0	SL		1.0	7
11	2022	48029	60434.0	80345.0	80323.0	4000.0	SL		1.0	8
12	2022	48032	60901.0	80338.0	80320.0	4000.0	SL		1.0	8
13	2022	48037	62617.0	80339.0	80321.0	4000.0	SL		1.0	8
14	2022	48039	61241.0	80340.0	80322.0	4000.0	SL		1.0	3
15	2022	48044	61408.0	80344.0	80322.0	4000.0	SL		1.0	4
16	2022	48053	61231.0	80341.0	80322.0	4000.0	SL		1.0	4
17	2022	48063	60258.0	80345.0	80323.0	4000.0	SL		1.0	8
18	2022	48076	60496.0	80347.0	80325.0	4000.0	SL		1.0	8
19	2022	48083	60306.0	80348.0	80327.0	4000.0	SL		1.0	7
20	2022	48085	60270.0	80348.0	80327.0	4000.0	SL		1.0	7
21	2022	48086	60302.0	80348.0	80327.0	4000.0	SL		1.0	7
22	2022	48116	60259.0	80338.0	80320.0	4000.0	SL		1.0	8
23	2022	48117	80487.0	80347.0	80325.0	4000.0	SL		1.0	8
24	2022	48120	60351.0	80337.0	80319.0	4000.0	SL		1.0	7
25	2022	48121	61558.0	80338.0	80320.0	4000.0	SL		1.0	8
26	2022	54242	5160.0	5160.0	5007.0	5001.0	SL		1.0	1
27	2022	71117	7043.0	7043.0	7095.0	7082.0	SL		1.0	1
28	2022	71163	7070.0	7070.0	7093.0	7082.0	SL		1.0	1
29	2022	88050	973.0	973.0	20207.0	20018.0	SL		1.0	1
30	2022	88080	1006.0	1006.0	20207.0	20018.0	SL		1.0	1
31	2022	88132	986.0	986.0	20211.0	20018.0	SL		1.0	1
32	2022	88142	20390.0	20390.0	20211.0	20018.0	SL		1.0	1
33	2022	88149	99718.0	99718.0	20210.0	20018.0	SL		1.0	1
34	2022	88155	99724.0	99724.0	20208.0	20018.0	SL		1.0	1
35	2022	88349	20282.0	20282.0	20208.0	20018.0	SL		1.0	1
36	2022	88350	20400.0	20400.0	20210.0	20018.0	SL		1.0	1
37	2022	88391	99725.0	99725.0	20208.0	20018.0	SL		1.0	1
38	2022	88394	99726.0	99726.0	20208.0	20018.0	SL		1.0	1
39	2022	88578	80226.0	80226.0	20210.0	20018.0	SL		1.0	1
40	2022	88595	80517.0	80517.0	20209.0	20018.0	SL		1.0	1
41	2022	88922	99768.0	99768.0	20208.0	20018.0	SL		1.0	1
XXXXXX
---------------------------------------------------------------------------
NameError                                 Traceback (most recent call last)
/tmp/ipykernel_780/3980108350.py in <cell line: 1>()
----> 1 XXXXXX

NameError: name 'XXXXXX' is not defined
# Create still_birth_2022 DataFrame
still_birth_2022 = df_nacrs_yr.filter((col('GENDER') == 'U') & (col('AGE_NUM') == 0)).orderBy('AM_CARE_KEY')
still_birth_2022.shape()
                                                                                
(0, 19)
# Merge DataFrames
merged_ucc = df_nacrs_yr.join(df_ucc, 'FACILITY_AM_CARE_NUM', 'inner').orderBy('FACILITY_AM_CARE_NUM')

# Create UCC_22 DataFrame
UCC_22 = merged_ucc


# Removing Duplicates and Stillbirths
ed_nodup_nosb_22 = df_nacrs_yr.filter(
    (col('ED_VISIT_IND_CODE') == '1') &
    (col('AMCARE_GROUP_CODE') == 'ED') &
    # (~col('AM_CARE_KEY').isin([row['AM_CARE_KEY'] for row in df_dups.select('AM_CARE_KEY').collect()])) &
    (~col('AM_CARE_KEY').isin([row['AM_CARE_KEY'] for row in still_birth_2022.select('AM_CARE_KEY').collect()]))
)

# Associate with SITE_ID/NAME/PEER and CORP_ID/NAME/PEER
ed_record_with_ucc_22_aa = ed_nodup_nosb_22.join(
    df_fac.select(
        'FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER',
        'CORP_ID', 'CORP_NAME', 'CORP_PEER', 'NEW_REGION_ID', 'REGION_E_DESC', 'PROVINCE_ID',
        'PROVINCE_NAME', 'NACRS_ED_FLG'
    ),
    on=['FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR'],
    how='left'
)

# Remove UCCs and Stillbirths
ed_nodup_noucc_nosb_22 = ed_nodup_nosb_22.join(
    UCC_22.select('FACILITY_AM_CARE_NUM'),
    on=['FACILITY_AM_CARE_NUM'],
    how='left_anti'
)
[Stage 95:======================================>                  (8 + 4) / 12]
ed_nodup_nosb_22 .shape()
                                                                                
(15129442, 19)
ed_nodup_noucc_nosb_22.shape()
                                                                                
(14397983, 19)
# from pyspark.sql.functions import col

# # Alias DataFrames
# ed_nodup_noucc_nosb_alias = ed_nodup_noucc_nosb_22.alias("ed")
# df_fac_alias = df_fac.alias("fac")

# # Perform the join using aliases
# ed_records_22_aa_df = ed_nodup_noucc_nosb_alias.join(
#     df_fac_alias,
#     col("ed.FACILITY_AM_CARE_NUM") == col("fac.FACILITY_AM_CARE_NUM"),
#     "left"
# )

# from pyspark.sql.functions import col

# # Alias DataFrames
# ed_alias = ed_nodup_noucc_nosb_22.alias("ed")
# fac_alias = df_fac.alias("fac")

# # Perform the join using aliases
# ed_records_22_aa_df = ed_alias.join(
#     fac_alias,
#     col("ed.FACILITY_AM_CARE_NUM") == col("fac.FACILITY_AM_CARE_NUM"),
#     "left"
# )

# # Select columns and handle duplicates
# # List all columns from ed_nodup_noucc_nosb_22 and selectively from df_fac to avoid duplicates
# selected_columns = [col(f"ed.{column_name}") for column_name in ed_nodup_noucc_nosb_22.columns]

# # Add columns from df_fac, renaming those that conflict
# conflicting_columns = ['FACILITY_AM_CARE_NUM']  # Add any other conflicting columns here
# for column_name in df_fac.columns:
#     if column_name not in conflicting_columns:
#         selected_columns.append(col(f"fac.{column_name}"))
#     else:
#         # If the column is conflicting, rename it (e.g., append '_fac')
#         selected_columns.append(col(f"fac.{column_name}").alias(f"{column_name}_fac"))

# # Construct the final DataFrame with selected columns
# ed_records_22_aa_df = ed_records_22_aa_df.select(*selected_columns)


from pyspark.sql.functions import col

# Assuming ed_records_22_aa_df is created from a join of ed_nodup_noucc_nosb_22 and df_fac
# and you need to handle potential duplicate 'SUBMISSION_FISCAL_YEAR' column

# Alias DataFrames
ed_alias = ed_nodup_noucc_nosb_22.alias("ed")
fac_alias = df_fac.alias("fac")

# Perform the join using aliases
ed_records_22_aa_df = ed_alias.join(
    fac_alias,
    col("ed.FACILITY_AM_CARE_NUM") == col("fac.FACILITY_AM_CARE_NUM"),
    "left"
)

# Select columns and handle duplicates
# List all columns from ed_nodup_noucc_nosb_22 and selectively from df_fac to avoid duplicates
selected_columns = [col(f"ed.{column_name}") for column_name in ed_nodup_noucc_nosb_22.columns]

# Add columns from df_fac, renaming those that conflict
conflicting_columns = ['FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR']  # Include 'SUBMISSION_FISCAL_YEAR' here
for column_name in df_fac.columns:
    if column_name not in conflicting_columns:
        selected_columns.append(col(f"fac.{column_name}"))
    else:
        # If the column is conflicting, rename it (e.g., append '_fac')
        selected_columns.append(col(f"fac.{column_name}").alias(f"{column_name}_fac"))

# Construct the final DataFrame with selected columns
ed_records_22_aa_df = ed_records_22_aa_df.select(*selected_columns)
ed_records_22_aa_df.shape()
                                                                                
(14397983, 39)
# Group by CORP_ID and count AM_CARE_KEY
ED_CORP_22_df = ed_records_22_aa_df.groupBy('CORP_ID').count().withColumnRenamed('count', 'ED_CNT')

# TPIA without UCC
ed_records_22_bb_df = ed_records_22_aa_df.filter(~col('VISIT_DISPOSITION').isin(['61', '63']))

# TPIA with UCC 
ed_records_with_ucc_22_bb_df = ed_record_with_ucc_22_aa.filter(~col('VISIT_DISPOSITION').isin(['61', '63']))

# LOS for admit without UCC
ed_records_admit_22_bb_df = ed_records_22_aa_df.filter(col('VISIT_DISPOSITION').isin(['06', '07']))

# LOS for admit with UCC 
ed_records_admit_with_ucc_22_bb_df = ed_record_with_ucc_22_aa.filter(col('VISIT_DISPOSITION').isin(['06', '07']))

ed_records_22_bb_df.shape()
                                                                                
(13281546, 39)
from pyspark.sql.functions import col

# Collect facility numbers to exclude based on 'TYPE' and 'IND'
exclude_facility_nums_for_TPIA = ed_facility_org.filter(
    (col('TYPE') == 'DQ') & (col('IND') == 'TPIA')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

exclude_types = ['SL', 'PS', 'DQ']
exclude_facility_nums = ed_facility_org.filter(
    col('TYPE').isin(exclude_types)
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

# Filter ed_records_22_bb_df DataFrame
ed_record = ed_records_22_bb_df.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums) &
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_for_TPIA)
)

# Collect facility numbers to exclude based on 'TYPE' and 'IND'
exclude_facility_nums_for_ELOS = ed_facility_org.filter(
    (col('TYPE') == 'DQ') & (col('IND') == 'ELOS')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

exclude_types = ['SL', 'PS', 'DQ']
exclude_facility_nums = ed_facility_org.filter(
    col('TYPE').isin(exclude_types)
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

# Create ed_record_admit_22 (LOS for admit without UCC for CORP LEVEL)
ed_record_admit_22 = ed_records_admit_22_bb_df.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums) &
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_for_ELOS)
)
                                                                                
ed_record_admit_22.shape()
                                                                                
(1612666, 39)
ed_record .shape()
                                                                                
(12811912, 39)
# For PEER group, remove 'SL' and specific facility numbers
exclude_facility_nums_sl = ed_facility_org.filter(
    (col('TYPE') == 'SL') & (col('FACILITY_AM_CARE_NUM') != '54242')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

exclude_facility_nums_dq = ed_facility_org.filter(
    (col('TYPE') == 'DQ') & (col('IND') == 'TPIA')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

# For the PEER group, remove UCC and standalone facilities
# Create ed_record 22_Peer

ed_record_22_Peer= ed_records_22_bb_df.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_sl) &
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_dq)
)

# Create ed_record_admit_22_Peer
ed_record_admit_22_Peer = ed_records_admit_22_bb_df.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_sl) &
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_dq)
)
ed_record_22_Peer.shape()
                                                                                
(12859285, 39)
ed_record_admit_22_Peer.shape()
                                                                                
(1618529, 39)
# Filter for ed_record_with_ucc dataset
exclude_facility_nums_tpia = ed_facility_org.filter(
    (col('TYPE') == 'DQ') & (col('IND') == 'TPIA')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

# Create ed_record_with_ucc_22 (TPIA with UCC for National, Provincial and Regional)
ed_record_with_ucc_22= ed_records_with_ucc_22_bb_df.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_tpia)
)

# Filter for ed_record_with_ucc dataset
exclude_facility_nums_los = ed_facility_org.filter(
    (col('TYPE') == 'DQ') & (col('IND') == 'ELOS')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

# Create ed_record_admit_with_ucc_22 (LOS with UCC for National, Provincial, and Regional)
ed_record_admit_with_ucc_22= ed_records_admit_with_ucc_22_bb_df.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_los)
)
ed_record_with_ucc_22.shape()
                                                                                
(13974509, 30)
ed_record_admit_with_ucc_22.shape()
                                                                                
(1629362, 30)
ed_records_22_bb_df.shape()
                                                                                
(13281546, 39)
ed_records_with_ucc_22_bb_df.shape()
                                                                                
(13974509, 30)
ed_records_admit_22_bb_df.shape()
                                                                                
(1623662, 39)
ed_records_admit_with_ucc_22_bb_df.shape()
                                                                                
(1629362, 30)
Step 2_IND_LOS_FY22
# Calculate LOS for corp level without UCC
los_org_cnt_22 = ed_record_admit_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID') \
    .agg(F.count('LOS_HOURS').alias('los_avl_cnt'))
los_rpt_org_22 = los_org_cnt_22.filter(los_org_cnt_22['los_avl_cnt'] >= 50)
los_supp_org_22 = los_org_cnt_22.filter(los_org_cnt_22['los_avl_cnt'] < 50)

# Calculate LOS for CORP level with UCC
los_org_cnt_ucc_22 = ed_record_admit_with_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID') \
    .agg(F.count('LOS_HOURS').alias('los_avl_cnt'))
los_rpt_org_ucc_22 = los_org_cnt_ucc_22.filter(los_org_cnt_ucc_22['los_avl_cnt'] >= 50)
los_supp_org_ucc_22 = los_org_cnt_ucc_22.filter(los_org_cnt_ucc_22['los_avl_cnt'] < 50)

# Calculate LOS for PEER level
los_peer_cnt_22 = ed_record_admit_22_Peer.groupBy('SUBMISSION_FISCAL_YEAR', 'SITE_PEER') \
    .agg(F.count('LOS_HOURS').alias('los_avl_cnt'))
los_rpt_peer_22 = los_peer_cnt_22.filter(los_peer_cnt_22['los_avl_cnt'] >= 50)
los_supp_peer_22 = los_peer_cnt_22.filter(los_peer_cnt_22['los_avl_cnt'] < 50)

# Calculate LOS for regional level with UCC
los_reg_cnt_22 = ed_record_admit_with_ucc_22.groupBy('NEW_REGION_ID') \
    .agg(F.count('LOS_HOURS').alias('los_avl_cnt'))
los_rpt_reg_22 = los_reg_cnt_22.filter(los_reg_cnt_22['los_avl_cnt'] >= 50)
los_supp_reg_22 = los_reg_cnt_22.filter(los_reg_cnt_22['los_avl_cnt'] < 50)

# Exclude corps with <5 records
los_org_supp_for_natprovreg = los_supp_org_ucc_22.filter(los_supp_org_ucc_22['los_avl_cnt'] < 5)
los_nt_record_ucc_22 = ed_record_admit_with_ucc_22.join(los_org_supp_for_natprovreg, 'CORP_ID', 'left_anti')

# Left join and select distinct rows
los_rpt_org_22_v1 = los_rpt_org_22.join(df_fac, 'CORP_ID', 'left').dropDuplicates()
los_rpt_reg_22_v1 = los_rpt_reg_22.join(df_fac, 'NEW_REGION_ID', 'left').dropDuplicates()
los_supp_org_v1 = los_supp_org_22.join(df_fac, 'CORP_ID', 'left').dropDuplicates()
los_supp_reg_v1 = los_supp_reg_22.join(df_fac, 'NEW_REGION_ID', 'left').dropDuplicates()

los_org_cnt_22 .shape()
                                                                                
(329, 3)
los_rpt_org_22.shape()
                                                                                
(315, 3)
los_supp_org_22.shape()
                                                                                
(14, 3)
los_org_cnt_ucc_22.shape()
                                                                                
(339, 3)
los_rpt_org_ucc_22.shape()
                                                                                
(322, 3)
los_supp_org_ucc_22.shape()
                                                                                
(17, 3)
los_peer_cnt_22.shape()
                                                                                
(4, 3)
los_rpt_peer_22.shape()
                                                                                
(4, 3)
los_supp_peer_22.shape()
                                                                                
(0, 3)
los_reg_cnt_22.shape()
                                                                                
(55, 2)
los_rpt_reg_22.shape()
                                                                                
(55, 2)
los_supp_reg_22.shape()
                                                                                
(0, 2)
los_org_supp_for_natprovreg.shape()
                                                                                
(3, 3)
los_nt_record_ucc_22.shape()
                                                                                
(1629354, 30)
Step 2_IND_TPIA_FY22
# For corp level without UCC
# Vectorized approach for 'tpia_rec' calculation


# Create 'tpia_rec' column with initial value 'Y'
ed_record = ed_record.withColumn('tpia_rec', F.lit('Y'))

# Update 'tpia_rec' based on conditions
ed_record = ed_record.withColumn('tpia_rec', 
                                 F.when(ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'].isNull() | 
                                        (ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'] == ''), 'B')
                                  .when(ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'N')
                                  .otherwise(ed_record['tpia_rec']))

# Filter records not equal to 'B'
tpia_org_cnt = ed_record.filter(ed_record['tpia_rec'] != 'B')

# Aggregation
tpia_org_rec = tpia_org_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
)

# Calculate percentage and sort
tpia_org_rec = tpia_org_rec.withColumn('tpia_rec_pct', col('tpia_calc_cnt') / col('Total_CASE'))
tpia_org_rec = tpia_org_rec.orderBy('CORP_ID')

# Conditional DataFrame creation
tpia_supp_org = tpia_org_rec.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
tpia_rpt_org = tpia_org_rec.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))

# For corp level with ucc
# Create a Dataframe tpia_org_cnt_ucc_22_a

# Create 'tpia_rec' column
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn('tpia_rec', 
                                                         F.when(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'N')
                                                          .when(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'].isNotNull() & 
                                                                (ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] != '9999'), 'Y')
                                                          .otherwise('B'))

# Filter out rows with 'tpia_rec' equal to "B"
tpia_org_cnt_ucc_22 = ed_record_with_ucc_22.filter(ed_record_with_ucc_22['tpia_rec'] != 'B')

# Aggregation
tpia_org_rec_ucc_22 = tpia_org_cnt_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn('tpia_rec_pct', col('tpia_calc_cnt') / col('Total_CASE'))

# Filter 'tpia_org_rec_ucc' based on conditions
tpia_supp_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
tpia_rpt_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))

tpia_org_cnt_ucc_22.shape()
                                                                                
(13412633, 31)
tpia_org_rec_ucc_22.shape()
                                                                                
(354, 6)
tpia_supp_org_ucc_22.shape()
                                                                                
(73, 6)
tpia_rpt_org_ucc_22.shape()
                                                                                
(352, 6)
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
# For Peer Level 
from pyspark.sql import functions as F

# Create 'tpia_rec' column with conditions
tpia_peer_cnt_a_df = ed_record_22_Peer.withColumn(
    'tpia_rec', 
    F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B')
     .when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
     .otherwise('Y')
)

# Filter out rows with 'tpia_rec' equal to 'B'
tpia_peer_cnt_df = tpia_peer_cnt_a_df.filter(F.col('tpia_rec') != 'B')

# Aggregating data for tpia_peer_rec_df
tpia_peer_rec_df = tpia_peer_cnt_df.groupBy('SUBMISSION_FISCAL_YEAR', 'SITE_PEER').agg(
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt'),
    (F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)) / F.count(F.lit(1))).alias('tpia_rec_pct')
)

# Creating tpia_supp_peer_df DataFrame
tpia_supp_peer_df = tpia_peer_rec_df.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)
tpia_peer_cnt_a_df.shape()
                                                                                
(12859285, 40)
tpia_peer_cnt_df.shape()
                                                                                
(12343830, 40)
tpia_peer_rec_df.shape()
                                                                                
(4, 5)
tpia_supp_peer_df.shape()
                                                                                
(0, 5)

# For Region
from pyspark.sql import functions as F
import numpy as np

# Create 'tpia_rec' column with conditions
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    'tpia_rec',
    F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B')
     .when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
     .otherwise('Y')
)

# Filter out rows with 'tpia_rec' equal to 'B'
tpia_reg_cnt = ed_record_with_ucc_22.filter(F.col('tpia_rec') != 'B')

# Aggregating data for tpia_reg_rec
tpia_reg_rec = tpia_reg_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'NEW_REGION_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt'),
    (F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)) / F.count(F.lit(1))).alias('tpia_rec_pct')
)

# Creating tpia_supp_reg and tpia_rpt_reg Dataframe using filter
tpia_supp_reg = tpia_reg_rec.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)
tpia_rpt_reg = tpia_reg_rec.filter(
    (F.col('tpia_calc_cnt') >= 50) | ((F.col('tpia_calc_cnt') < 50) & (F.col('tpia_rec_pct') >= 0.75))
)

# Save suppression reports for province 
tpia_rpt_reg_v1_df = tpia_rpt_reg.join(df_fac, tpia_rpt_reg['NEW_REGION_ID'] == df_fac['NEW_REGION_ID'], 'left').dropDuplicates()
tpia_supp_reg_v1_df = tpia_supp_reg.join(df_fac, tpia_supp_reg['NEW_REGION_ID'] == df_fac['NEW_REGION_ID'], 'left').dropDuplicates()
tpia_rpt_org_v1_df = tpia_rpt_org.join(df_fac, tpia_rpt_org['CORP_ID'] == df_fac['CORP_ID'], 'left').dropDuplicates()
tpia_supp_org_v1_df = tpia_supp_org.join(df_fac, tpia_supp_org['CORP_ID'] == df_fac['CORP_ID'], 'left').dropDuplicates()


Selection deleted
from pyspark.sql.functions import col

# Exclusion Rule for Region, Province, and National
tpia_org_supp_for_natprovreg_df = tpia_supp_org_ucc_22.filter(col('tpia_calc_cnt') < 5)

# Collect the list of CORP_IDs to be excluded
exclude_corp_ids = tpia_org_supp_for_natprovreg_df.select('CORP_ID').rdd.flatMap(lambda x: x).collect()

# Filter out the rows from ed_record_with_ucc_22
TPIA_nt_record_ucc_df = ed_record_with_ucc_22.filter(~col('CORP_ID').isin(exclude_corp_ids))

                                                                                
tpia_org_supp_for_natprovreg_df.shape()
                                                                                
(0, 6)
TPIA_nt_record_ucc_df.shape()
                                                                                
(13974509, 31)
tpia_reg_cnt.shape()
                                                                                
(13412633, 31)
tpia_reg_rec.shape()
                                                                                
(55, 6)
tpia_supp_reg.shape()
                                                                                
(4, 6)
tpia_rpt_reg.shape()
                                                                                
(55, 6)
tpia_rpt_reg_v1_df.shape()
                                                                                
(507, 26)
tpia_supp_reg_v1_df.shape()
                                                                                
(64, 26)
tpia_rpt_org_v1_df.shape()
                                                                                
(482, 26)
tpia_supp_org_v1_df.shape()
                                                                                
(73, 26)
STEP 3_IND_LOS_FY22:
Selection deleted
from pyspark.sql.functions import col, when

# Filter the DataFrame for ORGANIZATION_ID values in (2, 3, 4, 5)
filtered_data = organization_data.filter(col('ORGANIZATION_ID').isin([2, 3, 4, 5]))

# Apply the mapping to create the 'peer_code' column using when function
filtered_data = filtered_data.withColumn(
    'peer_code',
    when(col('ORGANIZATION_ID') == 2, 'T')
    .when(col('ORGANIZATION_ID') == 3, 'H1')
    .when(col('ORGANIZATION_ID') == 4, 'H2')
    .when(col('ORGANIZATION_ID') == 5, 'H3')
)

# Select and rename the desired columns
peer_desc_df = filtered_data.select(
    col('ORGANIZATION_ID').alias('peer_id'), 
    col('peer_code'), 
    col('ORGANIZATION_NAME_E_DESC').alias('peer_desc')
)

# Show the resulting DataFrame
peer_desc_df.show()


def percentile_ci_optimized(indata, percentile, confidence_interval=False):
    # Remove NaN values and sort if not already sorted
    clean_data = np.sort(indata[~np.isnan(indata)])

    ct = clean_data.size
    if ct > 0:
        kf = (ct - 1) * percentile
        pt_low_n = max(int(np.floor(kf)), 0)
        pt_upp_n = min(int(np.ceil(kf)), ct - 1)

        d = kf - np.floor(kf)
        point_est = clean_data[pt_low_n] * (1 - d) + clean_data[pt_upp_n] * d
        point_est = round(point_est * 10000) / 10000

        if confidence_interval:
            ci_index = 1.96 * np.sqrt(ct * percentile * (1 - percentile))
            ci_low_n = max(int(np.floor(ct * percentile - ci_index)) - 1, 0)
            ci_upp_n = min(int(np.ceil(ct * percentile + ci_index)) - 1, ct - 1)
            
            ci_low = clean_data[ci_low_n] if percentile not in [0, 1] else np.NaN
            ci_upp = clean_data[ci_upp_n] if percentile not in [0, 1] else np.NaN

            return point_est, ci_low, ci_upp
    else:
        point_est = ci_low = ci_upp = np.NaN

    return point_est if not confidence_interval else (point_est, ci_low, ci_upp)
from pyspark.sql.functions import col, percentile_approx

def calculate_percentile_spark(df, column, percentiles, bycols=None):
    if bycols:
        df = df.groupBy(*bycols).agg(*[percentile_approx(column, percentile).alias(f'percentile_{percentile}') for percentile in percentiles])
    else:
        df = df.agg(*[percentile_approx(column, percentile).alias(f'percentile_{percentile}') for percentile in percentiles])

    return df



from pyspark.sql.functions import upper

# Calculation for los_nt_22
los_nt_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1])
los_nt_22 = los_nt_22.select([upper(col(c)).alias(c) for c in los_nt_22.columns])

# Calculation for los_reg_22
los_reg_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])
los_reg_22 = los_reg_22.select([upper(col(c)).alias(c) for c in los_reg_22.columns])

# Calculation for los_prov_22
los_prov_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'PROVINCE_ID', 'PROVINCE_NAME'])
los_prov_22 = los_prov_22.select([upper(col(c)).alias(c) for c in los_prov_22.columns])

# Calculation for los_peer_22
los_peer_22 = calculate_percentile_spark(ed_record_admit_22_Peer, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'SITE_PEER'])
los_peer_22 = los_peer_22.select([upper(col(c)).alias(c) for c in los_peer_22.columns])

# Calculation for los_org_22
los_org_22 = calculate_percentile_spark(ed_record_admit_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_NAME', 'CORP_PEER'])
los_org_22 = los_org_22.select([upper(col(c)).alias(c) for c in los_org_22.columns])

# Calculation for los_site_22
los_site_22 = calculate_percentile_spark(ed_record_admit_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER'])
los_site_22 = los_site_22.select([upper(col(c)).alias(c) for c in los_site_22.columns])

# Filter and Rename for LOS_site_Huron_Perth
LOS_site_Huron_Perth = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))
LOS_site_Huron_Perth = LOS_site_Huron_Perth.select([upper(col(c)).alias(c) for c in LOS_site_Huron_Perth.columns])

# Additional Filtering and Renaming
filtered_rows = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))
filtered_rows = filtered_rows.select('SUBMISSION_FISCAL_YEAR', col('SITE_ID').alias('CORP_ID'), col('SITE_NAME').alias('CORP_NAME'), col('SITE_PEER').alias('CORP_PEER'), 'PERCENTILE_0.9')

# Concatenate DataFrames
los_org_22 = los_org_22.unionByName(filtered_rows)

---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_2055/2375582027.py in <cell line: 5>()
      3 # Calculation for los_nt_22
      4 los_nt_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1])
----> 5 los_nt_22 = los_nt_22.select([upper(col(c)).alias(c) for c in los_nt_22.columns])
      6 
      7 # Calculation for los_reg_22

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in select(self, *cols)
   3221         +-----+---+
   3222         """
-> 3223         jdf = self._jdf.select(self._jcols(*cols))
   3224         return DataFrame(jdf, self.sparkSession)
   3225 

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in __call__(self, *args)
   1320 
   1321         answer = self.gateway_client.send_command(command)
-> 1322         return_value = get_return_value(
   1323             answer, self.gateway_client, self.target_id, self.name)
   1324 

/usr/local/lib/python3.10/dist-packages/pyspark/errors/exceptions/captured.py in deco(*a, **kw)
    183                 # Hide where the exception came from that shows a non-Pythonic
    184                 # JVM exception message.
--> 185                 raise converted from None
    186             else:
    187                 raise

AnalysisException: [INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "percentile_0". Need a complex type [STRUCT, ARRAY, MAP] but got "DOUBLE".






from pyspark.sql.functions import col, upper, StringType

# Function to rename string columns to uppercase
def rename_columns_upper(df):
    return df.select([upper(col(c)).alias(c) if isinstance(df.schema[c].dataType, StringType) else col(c) for c in df.columns])

# Applying the percentile calculation and renaming columns
los_nt_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1])
los_nt_22 = rename_columns_upper(los_nt_22)

los_reg_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])
los_reg_22 = rename_columns_upper(los_reg_22)

los_prov_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'PROVINCE_ID', 'PROVINCE_NAME'])
los_prov_22 = rename_columns_upper(los_prov_22)

los_peer_22 = calculate_percentile_spark(ed_record_admit_22_Peer, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'SITE_PEER'])
los_peer_22 = rename_columns_upper(los_peer_22)

los_org_22 = calculate_percentile_spark(ed_record_admit_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_NAME', 'CORP_PEER'])
los_org_22 = rename_columns_upper(los_org_22)

los_site_22 = calculate_percentile_spark(ed_record_admit_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER'])
los_site_22 = rename_columns_upper(los_site_22)

# Filter and additional operations
LOS_site_Huron_Perth = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))
LOS_site_Huron_Perth = rename_columns_upper(LOS_site_Huron_Perth)

filtered_rows = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))
filtered_rows = filtered_rows.select('SUBMISSION_FISCAL_YEAR', col('SITE_ID').alias('CORP_ID'), col('SITE_NAME').alias('CORP_NAME'), col('SITE_PEER').alias('CORP_PEER'), 'percentile_0.9')

# Concatenate DataFrames
los_org_22 = los_org_22.unionByName(filtered_rows)

---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_2055/4090835778.py in <cell line: 9>()
      7 # Applying the percentile calculation and renaming columns
      8 los_nt_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1])
----> 9 los_nt_22 = rename_columns_upper(los_nt_22)
     10 
     11 los_reg_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])

/tmp/ipykernel_2055/4090835778.py in rename_columns_upper(df)
      3 # Function to rename string columns to uppercase
      4 def rename_columns_upper(df):
----> 5     return df.select([upper(col(c)).alias(c) if isinstance(df.schema[c].dataType, StringType) else col(c) for c in df.columns])
      6 
      7 # Applying the percentile calculation and renaming columns

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in select(self, *cols)
   3221         +-----+---+
   3222         """
-> 3223         jdf = self._jdf.select(self._jcols(*cols))
   3224         return DataFrame(jdf, self.sparkSession)
   3225 

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in __call__(self, *args)
   1320 
   1321         answer = self.gateway_client.send_command(command)
-> 1322         return_value = get_return_value(
   1323             answer, self.gateway_client, self.target_id, self.name)
   1324 

/usr/local/lib/python3.10/dist-packages/pyspark/errors/exceptions/captured.py in deco(*a, **kw)
    183                 # Hide where the exception came from that shows a non-Pythonic
    184                 # JVM exception message.
--> 185                 raise converted from None
    186             else:
    187                 raise

AnalysisException: [INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "percentile_0". Need a complex type [STRUCT, ARRAY, MAP] but got "DOUBLE".
