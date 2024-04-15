from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import FloatType, IntegerType


# Define a window specification
windowSpec = Window.partitionBy('CORP_ID')

# Create a GroupBy object and prepare DataFrame for results
grouped = los_org_all_yr_b.groupBy('CORP_ID').agg(
    F.collect_list('TIME').alias('TIME_LIST'),
    F.collect_list('PERCENTILE_90').alias('PERCENTILE_90_LIST')
)

all_results = []

# Iterate, perform regression, and store results
for row in grouped.collect():
    corp_id = row['CORP_ID']
    X = list(zip(row['TIME_LIST'], [1]*len(row['TIME_LIST'])))
    y = row['PERCENTILE_90_LIST']

    # Create a DataFrame from the data
    df = spark.createDataFrame(X, ["TIME", "const"]).withColumn("PERCENTILE_90", F.lit(None))
    
    # VectorAssembler
    assembler = VectorAssembler(inputCols=["TIME", "const"], outputCol="features")
    df = assembler.transform(df)
    
    # Linear Regression
    lr = LinearRegression(featuresCol='features', labelCol='PERCENTILE_90', regParam=0.0)
    model = lr.fit(df)
    
    # Extract coefficients and confidence intervals
    params = model.coefficients.toArray()
    std_err = model.summary.coefficientStandardErrors
    t_values = model.summary.tValues
    p_values = model.summary.pValues
    conf_int = model.summary.confidenceIntervals()

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [params, std_err, t_values, p_values,
                                   [conf_int[0][0]], [conf_int[0][1]]]):
        all_results.append({'CORP_ID': corp_id, '_TYPE_': param_type, 'TIME': float(values[0])})

# Convert to DataFrame and analyze results
los_org_trend_a = spark.createDataFrame(all_results)

# Add linr column
los_org_trend_a = los_org_trend_a.withColumn(
    'linr', F.when((F.col('_TYPE_') == 'PVALUE') & (F.col('TIME') < 0.05), 1).otherwise(0)
)

# Create subsets and merge
p_val = los_org_trend_a.filter(los_org_trend_a['_TYPE_'] == 'PVALUE').select('CORP_ID', 'linr')
parms = los_org_trend_a.filter(los_org_trend_a['_TYPE_'] == 'PARAMS').select('CORP_ID', 'TIME')

merged = p_val.join(parms, on='CORP_ID', how='inner')

# Define improvement indicators
merged = merged.withColumn('IMPROVEMENT_IND_CODE', F.lit('002')) \
               .withColumn('IMPROVEMENT_IND_E_DESC', F.lit('No Change'))

merged = merged.withColumn(
    'IMPROVEMENT_IND_CODE', 
    F.when((F.col('linr') == 1) & (F.col('TIME') > 0), '003')
    .when((F.col('linr') == 1) & (F.col('TIME') < 0), '001')
    .otherwise(F.col('IMPROVEMENT_IND_CODE'))
)

merged = merged.withColumn(
    'IMPROVEMENT_IND_E_DESC', 
    F.when((F.col('linr') == 1) & (F.col('TIME') > 0), 'Weakening')
    .when((F.col('linr') == 1) & (F.col('TIME') < 0), 'Improving')
    .otherwise(F.col('IMPROVEMENT_IND_E_DESC'))
)

# Final filtering
los_org_trend_b = merged.join(ed_nacrs_flg_1_22, on='CORP_ID', how='left_anti')


correct this error below 












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
spark_token = "d3eBH52f_FpZnRYFU80_vPWoMx8ynvVelhtk6OfHkLw="

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
                                                                                
df.shape()
cnt_by_prov=df_nacrs_yr.groupby("FACILITY_PROVINCE").count()
cnt_by_prov.show()
                                                                                
+-----------------+-------+
|FACILITY_PROVINCE|  count|
+-----------------+-------+
|               NS| 294976|
|               BC|1769621|
|               MB| 282416|
|               ON|6213469|
|               SK| 606903|
|               AB|2227674|
|               PE|  66002|
|               YT|  41740|
|               QC|3626641|
+-----------------+-------+

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
df_fac.show(n=2)
df_ucc.shape()
df_dq.shape()
df_ps.shape()
df_lookup.shape()
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
24/04/15 09:52:37 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
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
ed_facility_org_spark_pd  = ed_facility_org_spark.toPandas()
display(ed_facility_org_spark_pd)
# Create still_birth_2022 DataFrame
still_birth_2022 = df_nacrs_yr.filter((col('GENDER') == 'U') & (col('AGE_NUM') == 0)).orderBy('AM_CARE_KEY')
still_birth_2022.shape()
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
[Stage 95:==============================================>         (10 + 2) / 12]
ed_nodup_nosb_22 .shape()
ed_nodup_noucc_nosb_22.shape()
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
ed_record .shape()
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
ed_record_admit_22_Peer.shape()
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
ed_record_admit_with_ucc_22.shape()
ed_records_22_bb_df.shape()
ed_records_with_ucc_22_bb_df.shape()
ed_records_admit_22_bb_df.shape()
ed_records_admit_with_ucc_22_bb_df.shape()
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
los_nt_record_ucc_22 = ed_record_admit_with_ucc_22.join(los_org_supp_for_natprovreg, ed_record_admit_with_ucc_22['CORP_ID'] == los_org_supp_for_natprovreg['CORP_ID'], 'left_anti')

# Left join and select distinct rows
los_rpt_org_22_v1 = los_rpt_org_22.join(df_fac, 'CORP_ID', 'left').dropDuplicates()
los_rpt_reg_22_v1 = los_rpt_reg_22.join(df_fac, 'NEW_REGION_ID', 'left').dropDuplicates()
los_supp_org_v1 = los_supp_org_22.join(df_fac, 'CORP_ID', 'left').dropDuplicates()
los_supp_reg_v1 = los_supp_reg_22.join(df_fac, 'NEW_REGION_ID', 'left').dropDuplicates()

los_org_cnt_22 .shape()
los_rpt_org_22.shape()
los_supp_org_22.shape()
los_org_cnt_ucc_22.shape()
los_rpt_org_ucc_22.shape()
los_supp_org_ucc_22.shape()
los_peer_cnt_22.shape()
los_rpt_peer_22.shape()
los_supp_peer_22.shape()
los_reg_cnt_22.shape()
los_rpt_reg_22.shape()
los_supp_reg_22.shape()
los_org_supp_for_natprovreg.shape()
los_nt_record_ucc_22.shape()
los_nt_record_ucc_22.select('LOS_HOURS').show(20)
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
tpia_org_rec_ucc_22.shape()
tpia_supp_org_ucc_22.shape()
tpia_rpt_org_ucc_22.shape()
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
tpia_peer_cnt_df.shape()
tpia_peer_rec_df.shape()
tpia_supp_peer_df.shape()

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


from pyspark.sql.functions import col

# Exclusion Rule for Region, Province, and National
tpia_org_supp_for_natprovreg_df = tpia_supp_org_ucc_22.filter(col('tpia_calc_cnt') < 5)

# Collect the list of CORP_IDs to be excluded
exclude_corp_ids = tpia_org_supp_for_natprovreg_df.select('CORP_ID').rdd.flatMap(lambda x: x).collect()

# Filter out the rows from ed_record_with_ucc_22
TPIA_nt_record_ucc_22 = ed_record_with_ucc_22.filter(~col('CORP_ID').isin(exclude_corp_ids))

                                                                                
tpia_org_supp_for_natprovreg_df.shape()
TPIA_nt_record_ucc_22.shape()
tpia_reg_cnt.shape()
tpia_reg_rec.shape()
tpia_supp_reg.shape()
tpia_rpt_reg.shape()
tpia_rpt_reg_v1_df.shape()
tpia_supp_reg_v1_df.shape()
tpia_rpt_org_v1_df.shape()
tpia_supp_org_v1_df.shape()
STEP 3_IND_LOS_FY22:
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window

# def percentile_ci(df, metric, percentile, confidence_interval=False):
#     windowSpec = Window.orderBy(metric)
    
#     df = df.filter(F.col(metric).isNotNull())
#     df = df.withColumn("rank", F.percent_rank().over(windowSpec))
    
#     count = df.count()
    
#     if count > 0:
#         kf = (count - 1) * percentile
#         pt_low_n = F.floor(F.lit(kf)) + 1 - 1
#         pt_low_n = F.when(pt_low_n < 0, 0).otherwise(pt_low_n)
#         pt_upp_n = F.floor(F.lit(kf)) + 2 - 1
#         pt_upp_n = F.when(pt_upp_n > count - 1, count - 1).otherwise(pt_upp_n)
        
#         d = kf - F.floor(F.lit(kf))
        
#         point_est = (F.when(F.isnull(df.select(metric).collect()[pt_upp_n][metric]), 
#                             df.select(metric).collect()[pt_low_n][metric] - 
#                             df.select(metric).collect()[pt_low_n][metric] * d)
#                      .otherwise(df.select(metric).collect()[pt_low_n][metric] + 
#                                 d * (df.select(metric).collect()[pt_upp_n][metric] - 
#                                      df.select(metric).collect()[pt_low_n][metric])))
        
#         point_est = (F.round(point_est * 10000) / 10000).alias(f'point_estimate_{percentile}')
        
#         # Altman CI
#         ci_low_n = F.round(count * percentile - 1.96 * (count * percentile * (1 - percentile)) ** 0.5) - 1
#         ci_low_n = F.when(ci_low_n < 0, 0).otherwise(ci_low_n)
#         ci_upp_n = F.round(1 + count * percentile + 1.96 * (count * percentile * (1 - percentile)) ** 0.5) - 1
#         ci_upp_n = F.when(ci_upp_n > count - 1, count - 1).otherwise(ci_upp_n)
        
#         ci_low = F.when((percentile == 1) | (percentile == 0), None).otherwise(df.select(metric).collect()[ci_low_n][metric])
#         ci_upp = F.when((percentile == 1) | (percentile == 0), None).otherwise(df.select(metric).collect()[ci_upp_n][metric])
        
#     else:
#         point_est = None
#         ci_low = None
#         ci_upp = None

#     if confidence_interval:
#         return point_est, ci_low, ci_upp
#     else:
#         return point_est

# def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=[]):
#     strg = [str(round(100 * x)) if 100 * x == round(100 * x) else str(100 * x).replace('.', '_') for x in ppt]
#     for i in range(len(ppt)):
#         if not bycols:
#             df = df.withColumn('__', F.lit(1))
#             bycols = ['__']
        
#         df = percentile_ci(df, metric, ppt[i], confidence_interval)
        
#     return df


# los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
# los_regg.show()

# from pyspark.sql.functions import col, when

# # Filter the DataFrame for ORGANIZATION_ID values in (2, 3, 4, 5)
# filtered_data = organization_data.filter(col('ORGANIZATION_ID').isin([2, 3, 4, 5]))

# # Apply the mapping to create the 'peer_code' column using when function
# filtered_data = filtered_data.withColumn(
#     'peer_code',
#     when(col('ORGANIZATION_ID') == 2, 'T')
#     .when(col('ORGANIZATION_ID') == 3, 'H1')
#     .when(col('ORGANIZATION_ID') == 4, 'H2')
#     .when(col('ORGANIZATION_ID') == 5, 'H3')
# )

# # Select and rename the desired columns
# peer_desc_df = filtered_data.select(
#     col('ORGANIZATION_ID').alias('peer_id'), 
#     col('peer_code'), 
#     col('ORGANIZATION_NAME_E_DESC').alias('peer_desc')
# )

# # Show the resulting DataFrame
# peer_desc_df.show()

from pyspark.sql.functions import col, percentile_approx
# National 90th percentile exclude <5 and DQ
los_prov_22 = los_nt_record_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'PROVINCE_ID', 'FACILITY_PROVINCE') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))
los_prov_22.show()

los_nt_22 = los_nt_record_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))
los_nt_22.show()
                                                                                
+----------------------+-----------+-----------------+-------------+
|SUBMISSION_FISCAL_YEAR|PROVINCE_ID|FACILITY_PROVINCE|PERCENTILE_90|
+----------------------+-----------+-----------------+-------------+
|                  2022|     4000.0|               QC|         54.9|
|                  2022|     7082.0|               SK|         38.2|
|                  2022|     5001.0|               ON|         44.9|
|                  2022|     9001.0|               BC|         54.9|
|                  2022|      600.0|               MB|         55.7|
|                  2022|    20018.0|               AB|         36.4|
|                  2022|     2000.0|               NS|         54.5|
|                  2022|      100.0|               PE|         85.3|
|                  2022|    99003.0|               YT|         20.6|
+----------------------+-----------+-----------------+-------------+

[Stage 452:======================>                                 (4 + 6) / 10]
+----------------------+-------------+
|SUBMISSION_FISCAL_YEAR|PERCENTILE_90|
+----------------------+-------------+
|                  2022|         49.0|
+----------------------+-------------+

                                                                                
# Peer per Provincial 90th percentile exclude UCC and DQ
los_peer_prov_22 = ed_record_admit_22_Peer.groupBy('SUBMISSION_FISCAL_YEAR','SITE_PEER', 'FACILITY_PROVINCE') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))
los_peer_prov_22.show()
los_peer_prov_22.shape()

#Recehck H3 QC should be 32.1 and not 32. Check round up values.
                                                                                
+----------------------+---------+-----------------+------------------+
|SUBMISSION_FISCAL_YEAR|SITE_PEER|FACILITY_PROVINCE|     PERCENTILE_90|
+----------------------+---------+-----------------+------------------+
|                  2022|       H3|               ON|              27.2|
|                  2022|       H3|               AB|              26.0|
|                  2022|       H3|               SK|              14.1|
|                  2022|       H2|               ON|              45.6|
|                  2022|        T|               BC|              54.5|
|                  2022|       H3|               NS|              52.1|
|                  2022|       H2|               BC|46.800000000000004|
|                  2022|        T|               AB|              35.9|
|                  2022|       H3|               YT|              22.0|
|                  2022|       H1|               QC|              60.0|
|                  2022|       H2|               SK|              16.7|
|                  2022|       H1|               ON|              47.9|
|                  2022|       H1|               AB|              46.0|
|                  2022|       H2|               QC|              50.9|
|                  2022|       H2|               AB|              23.6|
|                  2022|       H1|               SK|              10.8|
|                  2022|        T|               NS|              51.1|
|                  2022|        T|               QC|53.800000000000004|
|                  2022|       H1|               BC|              59.9|
|                  2022|        T|               SK|              49.1|
+----------------------+---------+-----------------+------------------+
only showing top 20 rows

                                                                                
(29, 4)
#Correct colulm which has correct percentile fraction for percentile_90 of 32.1
# Peer per Provincial 90th percentile exclude UCC and DQ
los_peer_prov_22 = ed_record_admit_22_Peer.groupBy('SUBMISSION_FISCAL_YEAR','SITE_PEER', 'FACILITY_PROVINCE') \
                          .agg(percentile_approx(col('LOS_HOURS'), 0.9).alias('PERCENTILE_90'))
los_peer_prov_22.show(29)
los_peer_prov_22.shape()
#Recehck H3 QC should be 32.1 and not 32. Check round up values.
                                                                                
+----------------------+---------+-----------------+------------------+
|SUBMISSION_FISCAL_YEAR|SITE_PEER|FACILITY_PROVINCE|     PERCENTILE_90|
+----------------------+---------+-----------------+------------------+
|                  2022|       H3|               QC|              32.0|
|                  2022|       H3|               ON|              27.2|
|                  2022|       H3|               AB|              26.0|
|                  2022|       H3|               SK|              14.1|
|                  2022|       H2|               ON|              45.6|
|                  2022|        T|               AB|              35.9|
|                  2022|       H3|               BC|              51.2|
|                  2022|       H1|               QC|              60.0|
|                  2022|        T|               NS|              51.1|
|                  2022|       H2|               SK|              16.7|
|                  2022|        T|               QC|53.800000000000004|
|                  2022|       H2|               QC|              50.9|
|                  2022|        T|               ON|40.800000000000004|
|                  2022|       H1|               MB| 78.10000000000001|
|                  2022|       H2|               PE| 90.10000000000001|
|                  2022|        T|               BC|              54.5|
|                  2022|       H2|               BC|46.800000000000004|
|                  2022|       H1|               ON|              47.9|
|                  2022|       H2|               AB|              23.6|
|                  2022|       H1|               BC|              59.9|
|                  2022|        T|               MB|              52.4|
|                  2022|        T|               SK|              49.1|
|                  2022|       H2|               YT|20.400000000000002|
|                  2022|       H3|               NS|              52.1|
|                  2022|       H3|               YT|              22.0|
|                  2022|       H1|               AB|              46.0|
|                  2022|       H1|               SK|              10.8|
|                  2022|       H2|               NS|60.800000000000004|
|                  2022|       H1|               PE|              82.8|
+----------------------+---------+-----------------+------------------+

                                                                                
(29, 4)
# Peer Group 90th percentile exclude UCC, standalone, and DQ
los_peer_22 = ed_record_admit_22_Peer.groupBy('SUBMISSION_FISCAL_YEAR','SITE_PEER') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))
los_peer_22.show()
[Stage 569:=================================>                      (6 + 4) / 10]
+----------------------+---------+-------------+
|SUBMISSION_FISCAL_YEAR|SITE_PEER|PERCENTILE_90|
+----------------------+---------+-------------+
|                  2022|       H2|         47.5|
|                  2022|        T|         47.2|
|                  2022|       H1|         52.6|
|                  2022|       H3|         27.1|
+----------------------+---------+-------------+

                                                                                
# Regional 90th percentile exclude <5  and DQ
los_reg_22 = ed_record_admit_with_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'NEW_REGION_ID', 'REGION_E_DESC') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))
los_reg_22.show()
[Stage 577:==================================================>    (11 + 1) / 12]
+----------------------+-------------+--------------------+------------------+
|SUBMISSION_FISCAL_YEAR|NEW_REGION_ID|       REGION_E_DESC|     PERCENTILE_90|
+----------------------+-------------+--------------------+------------------+
|                  2022|      20209.0|Central Zone (Alta.)|              31.2|
|                  2022|      80323.0|    Outaouais Region|              90.9|
|                  2022|      80330.0|   Lanaudière Region|              51.7|
|                  2022|      80331.0|  Laurentides Region|              67.5|
|                  2022|      80327.0|Gaspésie–Îles-de-...|              47.1|
|                  2022|       5014.0|North West LHIN (...|              33.5|
|                  2022|       5007.0|Toronto Central L...|              45.7|
|                  2022|      81409.0|     South East Zone|              11.0|
|                  2022|      80154.0|Winnipeg Regional...|              62.5|
|                  2022|      80332.0|   Montérégie Region| 76.10000000000001|
|                  2022|      80320.0|Mauricie et Centr...|              38.4|
|                  2022|       5008.0|Central LHIN (For...|              40.1|
|                  2022|      20263.0|               Yukon|              20.6|
|                  2022|       5003.0|South West LHIN (...|              31.0|
|                  2022|       5015.0|Hamilton Niagara ...|51.300000000000004|
|                  2022|      20211.0|          North Zone|29.400000000000002|
|                  2022|       9057.0|       Island Health|              34.1|
|                  2022|      80289.0| Central Zone (N.S.)|              59.4|
|                  2022|       9056.0|Vancouver Coastal...|              43.0|
|                  2022|       5004.0|Waterloo Wellingt...|37.800000000000004|
+----------------------+-------------+--------------------+------------------+
only showing top 20 rows

                                                                                
los_site_22 = ed_record_admit_22.groupBy('SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))
los_site_22.show()
[Stage 592:============================>                            (1 + 1) / 2]
+----------------------+-------+--------------------+---------+------------------+
|SUBMISSION_FISCAL_YEAR|SITE_ID|           SITE_NAME|SITE_PEER|     PERCENTILE_90|
+----------------------+-------+--------------------+---------+------------------+
|                  2022|  120.0|Prince County Hos...|       H2| 90.10000000000001|
|                  2022|  122.0|Queen Elizabeth H...|       H1|              82.8|
|                  2022|  702.0|St. Boniface Hosp...|        T|              57.9|
|                  2022|  709.0|      Grace Hospital|       H1| 78.10000000000001|
|                  2022|  933.0|Athabasca Healthc...|       H3|              17.6|
|                  2022|  934.0|Barrhead Healthca...|       H3|              13.8|
|                  2022|  936.0|Bassano Health Ce...|       H3|30.400000000000002|
|                  2022|  937.0|Beaverlodge Munic...|       H3|              44.0|
|                  2022|  939.0|Big Country Hospital|       H3|              10.0|
|                  2022|  940.0|Covenant Health B...|       H3|              17.3|
|                  2022|  941.0|Bow Island Health...|       H3|               5.2|
|                  2022|  942.0|Boyle Healthcare ...|       H3|               8.8|
|                  2022|  944.0|Brooks Health Centre|       H3|              15.1|
|                  2022|  945.0|Alberta Children’...|        T|23.900000000000002|
|                  2022|  946.0|Foothills Medical...|        T|              28.2|
|                  2022|  947.0|Peter Lougheed Ce...|        T|              40.7|
|                  2022|  948.0|Rockyview General...|        T|              29.0|
|                  2022|  949.0|Canmore General H...|       H3|14.700000000000001|
|                  2022|  951.0|Cardston Health C...|       H3|              13.6|
|                  2022|  952.0|Central Peace Hea...|       H3|              29.1|
+----------------------+-------+--------------------+---------+------------------+
only showing top 20 rows

                                                                                
# Organization 90th percentile exclude <5 and DQ
los_org_22 = ed_record_admit_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_PEER') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))
los_org_22.show()
[Stage 612:============================>                            (1 + 1) / 2]
+----------------------+-------+---------+------------------+
|SUBMISSION_FISCAL_YEAR|CORP_ID|CORP_PEER|     PERCENTILE_90|
+----------------------+-------+---------+------------------+
|                  2022|  120.0|       H2| 90.10000000000001|
|                  2022|  122.0|       H1|              82.8|
|                  2022|  702.0|        T|              57.9|
|                  2022|  709.0|       H1| 78.10000000000001|
|                  2022|  933.0|       H3|              17.6|
|                  2022|  934.0|       H3|              13.8|
|                  2022|  936.0|       H3|30.400000000000002|
|                  2022|  937.0|       H3|              44.0|
|                  2022|  939.0|       H3|              10.0|
|                  2022|  940.0|       H3|              17.3|
|                  2022|  941.0|       H3|               5.2|
|                  2022|  942.0|       H3|               8.8|
|                  2022|  944.0|       H3|              15.1|
|                  2022|  945.0|        T|23.900000000000002|
|                  2022|  946.0|        T|              28.2|
|                  2022|  947.0|        T|              40.7|
|                  2022|  948.0|        T|              29.0|
|                  2022|  949.0|       H3|14.700000000000001|
|                  2022|  951.0|       H3|              13.6|
|                  2022|  952.0|       H3|              29.1|
+----------------------+-------+---------+------------------+
only showing top 20 rows

                                                                                
# Organization 90th percentile exclude <5 and DQ
los_org_22 = ed_record_admit_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_NAME','CORP_PEER') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))

# Filter rows based on specific 'SITE_ID' values
filtered_rows_los = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))

# Select and rename specific columns
filtered_rows_los = filtered_rows_los.select(
    col('SUBMISSION_FISCAL_YEAR'),
    col('SITE_ID').alias('CORP_ID'),
    col('SITE_NAME').alias('CORP_NAME'),
    col('SITE_PEER').alias('CORP_PEER'),
    col('Percentile_90')
)

# Union the datasets using unionByName with allowMissingColumns=True
los_org_22 = los_org_22.unionByName(filtered_rows_los, allowMissingColumns=True)

los_org_22.show()
los_org_22.shape()
                                                                                
+----------------------+-------+--------------------+---------+------------------+
|SUBMISSION_FISCAL_YEAR|CORP_ID|           CORP_NAME|CORP_PEER|     PERCENTILE_90|
+----------------------+-------+--------------------+---------+------------------+
|                  2022|  120.0|Prince County Hos...|       H2| 90.10000000000001|
|                  2022|  122.0|Queen Elizabeth H...|       H1|              82.8|
|                  2022|  702.0|St. Boniface Hosp...|        T|              57.9|
|                  2022|  709.0|      Grace Hospital|       H1| 78.10000000000001|
|                  2022|  933.0|Athabasca Healthc...|       H3|              17.6|
|                  2022|  934.0|Barrhead Healthca...|       H3|              13.8|
|                  2022|  936.0|Bassano Health Ce...|       H3|30.400000000000002|
|                  2022|  937.0|Beaverlodge Munic...|       H3|              44.0|
|                  2022|  939.0|Big Country Hospital|       H3|              10.0|
|                  2022|  940.0|Covenant Health B...|       H3|              17.3|
|                  2022|  941.0|Bow Island Health...|       H3|               5.2|
|                  2022|  942.0|Boyle Healthcare ...|       H3|               8.8|
|                  2022|  944.0|Brooks Health Centre|       H3|              15.1|
|                  2022|  945.0|Alberta Children’...|        T|23.900000000000002|
|                  2022|  946.0|Foothills Medical...|        T|              28.2|
|                  2022|  947.0|Peter Lougheed Ce...|        T|              40.7|
|                  2022|  948.0|Rockyview General...|        T|              29.0|
|                  2022|  949.0|Canmore General H...|       H3|14.700000000000001|
|                  2022|  951.0|Cardston Health C...|       H3|              13.6|
|                  2022|  952.0|Central Peace Hea...|       H3|              29.1|
+----------------------+-------+--------------------+---------+------------------+
only showing top 20 rows

                                                                                
(333, 5)




STEP 3_IND_TPIA_FY22:
# National 90th percentile TPIA

tpia_nt_22 = TPIA_nt_record_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR') \
                          .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
tpia_nt_22.show()

# Provincial 90th percentile TPIA
tpia_prov_22= TPIA_nt_record_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR','PROVINCE_ID', 'FACILITY_PROVINCE') \
                          .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
tpia_prov_22.show()

# Peer Group 90th  percentile TPIA
tpia_peer_22= ed_record_22_Peer.groupBy('SUBMISSION_FISCAL_YEAR','SITE_PEER') \
                          .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
tpia_peer_22.show()

# Regional 90th percentile TPIA
tpia_reg_22= ed_record_with_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR','FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC') \
                          .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
tpia_reg_22.show()

# Calculate for SITE_ID/NAME/PEER
tpia_site_22= ed_record.groupBy('SUBMISSION_FISCAL_YEAR','SITE_NAME', 'SITE_ID', 'SITE_PEER') \
                          .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
tpia_site_22.show()

# # Calculate for TPIA SITE_LEVEL
# TPIA_site_Huron_Perth= tpia_site_22.groupBy('SITE_ID') \
#                           .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
# TPIA_site_Huron_Perth.show()


# tpia_site_Huron_Perth = tpia_site_22.groupBy('SITE_ID').isin(5096,5099,5103,5209))
# tpia_site_Huron_Perth.show()

# Organization 90th percentile exclude <5 and DQ
tpia_org_22 = ed_record.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_PEER') \
                          .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
tpia_org_22.show()
tpia_org_22.shape()
                                                                                
+----------------------+-------------+
|SUBMISSION_FISCAL_YEAR|PERCENTILE_90|
+----------------------+-------------+
|                  2022|          5.0|
+----------------------+-------------+

                                                                                
+----------------------+-----------+-----------------+-------------+
|SUBMISSION_FISCAL_YEAR|PROVINCE_ID|FACILITY_PROVINCE|PERCENTILE_90|
+----------------------+-----------+-----------------+-------------+
|                  2022|     4000.0|               QC|         7.07|
|                  2022|     2000.0|               NS|         6.28|
|                  2022|     7082.0|               SK|         3.77|
|                  2022|     5001.0|               ON|         4.22|
|                  2022|     9001.0|               BC|          4.0|
|                  2022|    20018.0|               AB|         4.93|
|                  2022|    99003.0|               YT|         2.85|
|                  2022|      600.0|               MB|         7.47|
|                  2022|      100.0|               PE|         7.68|
+----------------------+-----------+-----------------+-------------+

                                                                                
+----------------------+---------+-------------+
|SUBMISSION_FISCAL_YEAR|SITE_PEER|PERCENTILE_90|
+----------------------+---------+-------------+
|                  2022|       H2|         4.95|
|                  2022|        T|         5.62|
|                  2022|       H1|         5.12|
|                  2022|       H3|         3.63|
+----------------------+---------+-------------+

                                                                                
+----------------------+-----------------+-------------+--------------------+------------------+
|SUBMISSION_FISCAL_YEAR|FACILITY_PROVINCE|NEW_REGION_ID|       REGION_E_DESC|     PERCENTILE_90|
+----------------------+-----------------+-------------+--------------------+------------------+
|                  2022|               BC|       9058.0|     Northern Health|              2.45|
|                  2022|               NS|      80289.0| Central Zone (N.S.)|6.2700000000000005|
|                  2022|               QC|      80331.0|  Laurentides Region|             10.17|
|                  2022|               BC|       9057.0|       Island Health|              3.87|
|                  2022|               AB|      20211.0|          North Zone|              2.73|
|                  2022|               ON|       5003.0|South West LHIN (...|               3.7|
|                  2022|               QC|      80318.0|Saguenay–Lac-Sain...|              6.45|
|                  2022|               BC|       9055.0|       Fraser Health|               4.5|
|                  2022|               SK|      81406.0|North Central Eas...|              2.98|
|                  2022|               ON|       5004.0|Waterloo Wellingt...|              4.82|
|                  2022|               ON|       5013.0|North East LHIN (...|              4.17|
|                  2022|               ON|       5012.0|North Simcoe Musk...|              4.03|
|                  2022|               PE|      20034.0|          Health PEI|              7.68|
|                  2022|               QC|      80325.0|    Côte-Nord Region|              6.92|
|                  2022|               QC|      80320.0|Mauricie et Centr...|              7.98|
|                  2022|               SK|      81407.0|      Saskatoon Zone|              3.18|
|                  2022|               QC|      80329.0|        Laval Region|              7.97|
|                  2022|               ON|       5005.0|Central West LHIN...|2.8000000000000003|
|                  2022|               NS|       2007.0|   IWK Health Centre|              4.53|
|                  2022|               BC|       9059.0|Provincial Health...|              5.37|
+----------------------+-----------------+-------------+--------------------+------------------+
only showing top 20 rows

                                                                                
+----------------------+--------------------+-------+---------+------------------+
|SUBMISSION_FISCAL_YEAR|           SITE_NAME|SITE_ID|SITE_PEER|     PERCENTILE_90|
+----------------------+--------------------+-------+---------+------------------+
|                  2022|Abbotsford Region...|99320.0|       H1|               6.3|
|                  2022|Aberdeen Hospital...| 2011.0|       H2|               7.0|
|                  2022|  Alexandra Hospital| 5058.0|       H3|              3.95|
|                  2022|Arcola Health Centre| 7003.0|       H3|               3.0|
|                  2022|Arnprior Regional...| 5019.0|       H3|              3.97|
|                  2022|Athabasca Healthc...|  933.0|       H3|1.9000000000000001|
|                  2022|Atikokan General ...| 5020.0|       H3|              1.45|
|                  2022|Barrhead Healthca...|  934.0|       H3|              1.47|
|                  2022|Brant Com HcSys—B...|99009.0|       H1|              5.92|
|                  2022|CHU Sainte-Justin...|60144.0|        T|              8.03|
|                  2022|CHU de Québec - U...|61425.0|        T|5.0200000000000005|
|                  2022|CISSS de Chaudièr...|61282.0|       H2|              3.97|
|                  2022|CISSS de Chaudièr...|61274.0|       H2|              3.38|
|                  2022|CISSS de Chaudièr...|80103.0|       H1|              3.38|
|                  2022|CISSS de l'Abitib...|80105.0|       H3|              2.13|
|                  2022|CISSS de l'Abitib...|61277.0|       H2|               8.8|
|                  2022|CISSS de l'Abitib...|61041.0|       H2|              3.93|
|                  2022|CISSS de l'Abitib...|61301.0|       H2|              9.42|
|                  2022|CISSS de l'Outaou...|61270.0|       H2|              5.62|
|                  2022|CISSS de la Côte-...|61304.0|       H2|              9.23|
+----------------------+--------------------+-------+---------+------------------+
only showing top 20 rows

                                                                                
+----------------------+-------+---------+------------------+
|SUBMISSION_FISCAL_YEAR|CORP_ID|CORP_PEER|     PERCENTILE_90|
+----------------------+-------+---------+------------------+
|                  2022| 5183.0|       H1|              4.65|
|                  2022| 7035.0|       H3|              1.87|
|                  2022| 5400.0|       H1|              5.92|
|                  2022| 5028.0|       H3|              3.72|
|                  2022| 9087.0|        T|              3.33|
|                  2022|20392.0|       H1|              4.18|
|                  2022|80478.0|       H1|              1.17|
|                  2022| 1095.0|       H1|              4.38|
|                  2022|80348.0|       H2|5.1000000000000005|
|                  2022| 1036.0|       H3|              2.92|
|                  2022|62704.0|        T|               9.1|
|                  2022| 7019.0|       H3|              1.03|
|                  2022| 2054.0|       H2|              7.92|
|                  2022| 9071.0|       H2|              2.48|
|                  2022| 1050.0|       H3|              2.65|
|                  2022|99331.0|       H3|              2.23|
|                  2022|  965.0|       H3|              4.43|
|                  2022| 1026.0|       H3|               1.8|
|                  2022| 5068.0|       H2|              4.33|
|                  2022|  971.0|       H3|              2.37|
+----------------------+-------+---------+------------------+
only showing top 20 rows

                                                                                
(330, 4)
# Filter rows based on specific 'SITE_ID' values
tpia_site_Huron_Perth = tpia_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))
tpia_site_Huron_Perth.show()
[Stage 811:============================>                            (1 + 1) / 2]
+----------------------+--------------------+-------+---------+-------------+
|SUBMISSION_FISCAL_YEAR|           SITE_NAME|SITE_ID|SITE_PEER|PERCENTILE_90|
+----------------------+--------------------+-------+---------+-------------+
|                  2022|Huron Perth Hc Al...| 5209.0|       H3|         2.02|
|                  2022|Huron Perth Hc Al...| 5099.0|       H3|         2.02|
|                  2022|Huron Perth Hc Al...| 5096.0|       H3|         2.95|
|                  2022|Huron Perth Hc Al...| 5103.0|       H2|         3.12|
+----------------------+--------------------+-------+---------+-------------+

                                                                                
# Organization 90th percentile exclude <5 and DQ
tpia_org_22 = ed_record.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_NAME','CORP_PEER') \
                          .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
# tpia_org_22.show()
# tpia_org_22.shape()


# Filter rows based on specific 'SITE_ID' values
filtered_rows_tpia = tpia_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))

# Select and rename specific columns
filtered_rows_tpia = filtered_rows_tpia.select(
    col('SUBMISSION_FISCAL_YEAR'),
    col('SITE_ID').alias('CORP_ID'),
    col('SITE_NAME').alias('CORP_NAME'),
    col('SITE_PEER').alias('CORP_PEER'),
    col('Percentile_90')
)

# Union the datasets using unionByName with allowMissingColumns=True
tpia_org_22 = tpia_org_22.unionByName(filtered_rows_tpia, allowMissingColumns=True)

tpia_org_22.show()
tpia_org_22.shape()
                                                                                
+----------------------+-------+--------------------+---------+------------------+
|SUBMISSION_FISCAL_YEAR|CORP_ID|           CORP_NAME|CORP_PEER|     PERCENTILE_90|
+----------------------+-------+--------------------+---------+------------------+
|                  2022| 7019.0|Hudson Bay Health...|       H3|              1.03|
|                  2022|80144.0|Arnprior Regional...|       H3|              3.97|
|                  2022| 5028.0|Campbellford Memo...|       H3|              3.72|
|                  2022|62722.0|Centre universita...|        T|              7.57|
|                  2022|20392.0|Trillium Health P...|       H1|              4.18|
|                  2022| 1038.0|WestView Health C...|       H3|              5.22|
|                  2022|10020.0|Niagara Health Sy...|       H1|              4.63|
|                  2022|80337.0|CIUSSS de la Capi...|       H3|              4.95|
|                  2022| 1096.0|   Oak Valley Health|       H1|              2.52|
|                  2022| 2054.0|South Shore Regio...|       H2|              7.92|
|                  2022|81263.0|Guelph General Ho...|       H1|              4.88|
|                  2022| 5177.0|Grey Bruce Health...|       H2|               3.6|
|                  2022|  955.0|Cold Lake Healthc...|       H3|              3.98|
|                  2022|80364.0|North of Superior...|       H3|1.8800000000000001|
|                  2022|  972.0|Fairview Health C...|       H3|              2.95|
|                  2022|80356.0|CISSS de la Monté...|       H1|               6.8|
|                  2022| 5076.0|Hôpital de Mattaw...|       H3|              1.58|
|                  2022|80380.0|Kingston Health S...|        T|              4.45|
|                  2022|80258.0|Hamilton Health S...|        T|               5.4|
|                  2022| 5141.0|Thunder Bay Regio...|        T|3.8000000000000003|
+----------------------+-------+--------------------+---------+------------------+
only showing top 20 rows

                                                                                
(334, 5)
STEP 4_IND_LOS_TPIA_FY22:



# # # # Create standalone DataFrame
# # # stand_alone = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select('CORP_ID')

# # # Create standalone DataFrame
# # stand_alone = df_fac.filter(col('NACRS_ED_FLG') == 1).select('CORP_ID').distinct()

# # Create multiple_sites DataFrame
# multiple_sites = df_fac.join(stand_alone, 'CORP_ID')
# multiple_sites = multiple_sites.groupBy('CORP_ID').agg(count('*').alias('cnt_sites'))
# multiple_sites = multiple_sites.filter(col('cnt_sites') > 1)

# # # Create standalone DataFrame
stand_alone = df_fac.filter(col('NACRS_ED_FLG') == 1).select('CORP_ID').distinct()
# # Create standalone DataFrame
# stand_alone = df_fac.filter(col('NACRS_ED_FLG') == 1).select('CORP_ID')
from pyspark.sql.functions import col, count

# Create multiple_sites DataFrame
multiple_sites = df_fac.join(stand_alone, 'CORP_ID')
multiple_sites = multiple_sites.groupBy('CORP_ID').agg(count('*').alias('cnt_sites'))
multiple_sites = multiple_sites.filter(col('cnt_sites') > 1)

# # # Create standalone DataFrame
# stand_alone = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select('CORP_ID').distinct()

# # Create multiple_sites DataFrame
# multiple_sites = df_fac.join(stand_alone, 'CORP_ID', 'inner') \
#     .groupBy('CORP_ID').agg(count('*').alias('cnt_sites')) \
#     .filter(col('cnt_sites') > 1)

# # Create standalone DataFrame
# # stand_alone = df_fac.filter(col('NACRS_ED_FLG') == 1).select('CORP_ID')

# # Create multiple_sites DataFrame
# multiple_sites = df_fac.join(stand_alone, 'CORP_ID', 'inner') \
#     .groupBy('CORP_ID').agg(count('*').alias('cnt_sites')) \
#     .filter(col('cnt_sites') > 1)





# stand_alone = df_fac[df_fac['NACRS_ED_FLG'] == 1][['CORP_ID']]
stand_alone.shape()
stand_alone.show()
multiple_sites.show()
from pyspark.sql.functions import col

# Alias the datasets 
df_fac_alias = df_fac.alias("fac")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Filter ed_facility_org for specific conditions and alias the result
ed_facility_org_filtered = ed_facility_org_alias.filter(
    (col("edfo.TYPE") == 'SL') & (col("edfo.CORP_CNT") == 1)
).select(col("edfo.CORP_ID").alias("CORP_ID_edfo"))

# Perform the join operation
ed_nacrs_flg_1_22 = df_fac_alias.join(
    ed_facility_org_filtered, 
    col("fac.CORP_ID") == col("CORP_ID_edfo")
).filter(col("fac.NACRS_ED_FLG") == 1)

# Recreate duplicates
ed_nacrs_flg_1_22_with_duplicates = ed_nacrs_flg_1_22.union(ed_nacrs_flg_1_22.withColumnRenamed("CORP_ID", "CORP_ID_2"))

# Drop duplicates
ed_nacrs_flg_1_22 = ed_nacrs_flg_1_22_with_duplicates.dropDuplicates()

ed_nacrs_flg_1_22.printSchema()
ed_nacrs_corp_ids = ed_nacrs_flg_1_22.select('CORP_ID')
tpia_supp_corp_ids = tpia_supp_org.select('CORP_ID')
los_supp_corp_ids = los_supp_org_22.select('CORP_ID')

ed_facility_filtered_corp_ids = ed_facility_org.filter(
    (col('SUBMISSION_FISCAL_YEAR') == "2022") &
    (
        (col('TYPE') == 'PS') & (col('CORP_CNT') == 1) |
        (col('TYPE') == 'DQ') & (col('CORP_CNT') == 1) & (col('IND') == 'TPIA')
    )
).select('CORP_ID')

#Perform the join operation on tpia_org_22 and ed_nacrs_corp_ids, tpia_supp_corp_ids, and ed_facility_filtered_corps_ids
tpia_org_ta = tpia_org_22.join(ed_nacrs_corp_ids, 'CORP_ID', 'left_anti') \
    .join(tpia_supp_corp_ids, 'CORP_ID', 'left_anti') \
    .join(ed_facility_filtered_corp_ids, 'CORP_ID', 'left_anti')

# Remove Huron Perth Healthcare Alliance, corps_id = 80228 form TPIA  
tpia_org_ta = tpia_org_ta.filter((col('CORP_ID') != 80228) & tpia_org_ta['CORP_PEER'].isNotNull())
# los_org_ta = los_org_ta.filter((col('CORP_ID') != 80228) & los_org_ta['CORP_PEER'].isNotNull())

# Sort DataFrames by CORP_ID
tpia_org_ta = tpia_org_ta.orderBy('CORP_ID')
# los_org_ta = tpia_org_ta.orderBy('CORP_ID')
tpia_org_ta.shape()
excluded_corp_ids = ed_nacrs_flg_1_22.select('CORP_ID').union(
    los_supp_org_22.select('CORP_ID')
).union(
    ed_facility_org.filter(
        (col('SUBMISSION_FISCAL_YEAR') == "2022") &
        (((col('TYPE') == 'PS') & (col('CORP_CNT') == 1)) |
         ((col('TYPE') == 'DQ') & (col('CORP_CNT') == 1) & (col('IND') == 'ELOS')))
    ).select('CORP_ID')
).distinct()

# Remove suppressed corp and non-reported corps for ELOS
los_org_ta = los_org_22.join(excluded_corp_ids, 'CORP_ID', 'left_anti')

# Remove specific CORP_ID and check for not null in CORP_PEER
los_org_ta = los_org_ta.filter((col('CORP_ID') != 80228) & col('CORP_PEER').isNotNull())


# Sort DataFrames
los_org_ta = los_org_ta.orderBy('CORP_ID')

los_org_ta.shape()
from pyspark.sql.functions import col 

tpia_reg_22_ta = tpia_reg_22.join(
    tpia_supp_reg, 
    tpia_reg_22['NEW_REGION_ID'] == tpia_supp_reg['NEW_REGION_ID'],
    'left_anti'
)
los_reg_22_ta = los_reg_22.join(
    los_supp_reg_22, 
    los_reg_22['NEW_REGION_ID'] == los_reg_22['NEW_REGION_ID'],
    'left_anti'
)
24/04/15 09:56:34 WARN Column: Constructing trivially true equals predicate, 'NEW_REGION_ID#5357 = NEW_REGION_ID#5357'. Perhaps you need to use aliases.
from pyspark.sql.functions import expr

# Calculate percentiles for TPIA_org
prct_20_80_tpia_org_22_ta = tpia_org_ta \
    .groupBy("CORP_PEER") \
    .agg(expr("percentile(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile(PERCENTILE_90, 0.8)").alias("80th_Percentile"))

# Calculate percentiles for ELOS_org
prct_20_80_los_org_22_ta = los_org_ta \
    .groupBy("CORP_PEER") \
    .agg(expr("percentile(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile(PERCENTILE_90, 0.8)").alias("80th_Percentile"))


# Calculate percentiles for TPIA_reg
prct_20_80_tpia_reg_22_ta = tpia_reg_22_ta \
    .groupBy("SUBMISSION_FISCAL_YEAR") \
    .agg(expr("percentile(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile(PERCENTILE_90, 0.8)").alias("80th_Percentile"))


# Calculate percentiles for ELOS_reg
prct_20_80_los_reg_22_ta = los_reg_22_ta \
    .groupBy("SUBMISSION_FISCAL_YEAR") \
    .agg(expr("percentile(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile(PERCENTILE_90, 0.8)").alias("80th_Percentile"))


prct_20_80_tpia_org_22_ta.show()
                                                                                
+---------+------------------+------------------+
|CORP_PEER|   20th_Percentile|   80th_Percentile|
+---------+------------------+------------------+
|       H1|             3.206| 6.874000000000001|
|       H3|               1.8|3.3200000000000003|
|        T|3.8000000000000003|              7.43|
|       H2|3.0460000000000003|              4.98|
+---------+------------------+------------------+

prct_20_80_los_org_22_ta.show()
                                                                                
+---------+------------------+------------------+
|CORP_PEER|   20th_Percentile|   80th_Percentile|
+---------+------------------+------------------+
|       H1|31.900000000000002|              67.5|
|       H3| 9.559999999999999|26.040000000000006|
|        T|              28.2|              54.7|
|       H2|15.840000000000002|             45.88|
+---------+------------------+------------------+

prct_20_80_tpia_reg_22_ta.show()
[Stage 1442:=====================================>                  (2 + 1) / 3]
+----------------------+---------------+---------------+
|SUBMISSION_FISCAL_YEAR|20th_Percentile|80th_Percentile|
+----------------------+---------------+---------------+
|                  2022|           3.45|           7.07|
+----------------------+---------------+---------------+

                                                                                
prct_20_80_los_reg_22_ta.show()
[Stage 1473:========================================>              (8 + 3) / 11]
+----------------------+------------------+------------------+
|SUBMISSION_FISCAL_YEAR|   20th_Percentile|   80th_Percentile|
+----------------------+------------------+------------------+
|                  2022|28.140000000000004|59.620000000000005|
+----------------------+------------------+------------------+

                                                                                
# Copy the results to *_base DataFrames
tpia_peer_base = prct_20_80_tpia_org_22_ta
los_peer_base = prct_20_80_los_org_22_ta
tpia_reg_base = prct_20_80_tpia_reg_22_ta
los_reg_base = prct_20_80_los_reg_22_ta


# Read CSV file into a DataFrame
los_reg_21= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/los_reg_21.csv")

los_prov_21= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/los_prov_21.csv")


los_org_21= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/los_org_21.csv")

los_reg_20= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/los_reg_20.csv")

los_prov_20= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/los_prov_20.csv")

los_org_20= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/los_org_20.csv")

# Read CSV file into a DataFrame
tpia_reg_21= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/tpia_reg_21.csv")

tpia_prov_21= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/tpia_prov_21.csv")

tpia_org_21= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/tpia_org_21.csv")

tpia_reg_20= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/tpia_reg_20.csv")

tpia_prov_20= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/tpia_prov_20.csv")

tpia_org_20= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/workbooks/tpia_org_20.csv")

                                                           .otherwise(F.col('CORP_ID')))
# # Update data for specific CORP_ID values
# corp_id_mapping = {
#     1019: 81170,
#     10038: 81124,
#     7077: 80960,
#     5045: 81131,
#     5085: 81180,
#     5049: 81263,
#     5160: None
# }

# # Apply the mapping to CORP_ID columns in all DataFrames
# dataframes = [los_org_21, los_org_20, los_org_22_a, tpia_org_21, tpia_org_20, tpia_org_22_a]
# for df in dataframes:
#     for col, val in corp_id_mapping.items():
#         df = df.withColumn('CORP_ID', F.when(F.col('CORP_ID') == col, val).otherwise(F.col('CORP_ID')))


#    corp_id corp_peer
# 0  81131.0        H2
# 1  81170.0        H1
# 2  81263.0        H1- up
# 3  81124.0        H1
# 4  81180.0         T- up


from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Apply transformations
tpia_org_21 = tpia_org_21.withColumn("CORP_ID", 
                                     when(col("CORP_ID") == 1019, 81170)
                                     .when(col("CORP_ID") == 10038, 81124)
                                     .when(col("CORP_ID") == 7077, 80960)
                                     .when(col("CORP_ID") == 5045, 81131)
                                     .when(col("CORP_ID") == 5085, 81180)
                                     .when(col("CORP_ID") == 5049, 81263)
                                     .otherwise(col("CORP_ID")))

tpia_org_20 = tpia_org_20.withColumn("CORP_ID", 
                                     when(col("CORP_ID") == 1019, 81170)
                                     .when(col("CORP_ID") == 10038, 81124)
                                     .when(col("CORP_ID") == 7077, 80960)
                                     .when(col("CORP_ID") == 5045, 81131)
                                     .when(col("CORP_ID") == 5085, 81180)
                                     .when(col("CORP_ID") == 5049, 81263)
                                     .otherwise(col("CORP_ID")))

tpia_org_22 = tpia_org_22.filter(col("CORP_ID") != 5160).withColumnRenamed("percentile_90", "PERCENTILE_90")

#Rename columns 
tpia_org_21 = tpia_org_21.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")
tpia_org_20 = tpia_org_20.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")

# Apply transformations for los
los_org_21 = los_org_21.withColumn("CORP_ID", 
                                   when(col("CORP_ID") == 1019, 81170)
                                   .when(col("CORP_ID") == 10038, 81124)
                                   .when(col("CORP_ID") == 7077, 80960)
                                   .when(col("CORP_ID") == 5045, 81131)
                                   .when(col("CORP_ID") == 5085, 81180)
                                   .when(col("CORP_ID") == 5049, 81263)
                                   .otherwise(col("CORP_ID")))

los_org_20 = los_org_20.withColumn("CORP_ID", 
                                   when(col("CORP_ID") == 1019, 81170)
                                   .when(col("CORP_ID") == 10038, 81124)
                                   .when(col("CORP_ID") == 7077, 80960)
                                   .when(col("CORP_ID") == 5045, 81131)
                                   .when(col("CORP_ID") == 5085, 81180)
                                   .when(col("CORP_ID") == 5049, 81263)
                                   .otherwise(col("CORP_ID")))

los_org_22 = los_org_22.filter(col("CORP_ID") != 5160).withColumnRenamed("percentile_90", "PERCENTILE_90")

# # Rename column
los_org_21 = los_org_21.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")
los_org_20 = los_org_20.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")



# Rename the 'PEER_GROUP_ID' column to 'CORP_PEER' in los_org_21_spark and los_org_20_spark &
# Rename the fiscal_year column to 'SUBMISSION_FISCAL_YEAR in los_reg_21_spark and tpia_reg_21
los_org_21 = los_org_21.withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
los_org_20 = los_org_20.withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
tpia_org_21 = tpia_org_21.withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
tpia_org_20 = tpia_org_20.withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

los_reg_21 = los_reg_21.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR')
tpia_reg_21 = tpia_reg_21.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR')

# Filter out rows where CORP_ID is 5160
los_org_21 = los_org_21.filter(F.col('CORP_ID') != 5160)
los_org_20 = los_org_20.filter(F.col('CORP_ID') != 5160)

# Merge los_org_22_a_spark with los_peer_base_spark on 'CORP_PEER' column
los_org_cmp = los_org_22_a.join(los_peer_base, on='CORP_PEER', how='inner')

# Merge tpia_org_22_a_spark with tpia_peer_base_spark on 'CORP_PEER' column
tpia_org_cmp = tpia_org_22_a.join(tpia_peer_base, on='CORP_PEER', how='inner')

# Merge los_reg_22_a_spark with los_reg_base_spark on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
los_reg_cmp = los_reg_22_a.join(los_reg_base, on='SUBMISSION_FISCAL_YEAR', how='inner')

# Merge tpia_reg_22_spark with tpia_reg_base_spark on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
tpia_reg_cmp = tpia_reg_22_a.join(tpia_reg_base, on='SUBMISSION_FISCAL_YEAR', how='inner')



tpia_org_22.shape()
tpia_org_22_a.shape()
los_org_22_a.shape()
los_org_22.shape()
los_org_cmp.shape()
tpia_org_cmp.shape()
los_org_cmp.shape()
# los_org_cmp.write.csv('los_org_cmp_spark.csv', header = True, mode='overwrite')
# Rename columns for each DataFrame
renamed_columns = {
    '0.2': '20th_Percentile',
    '0.8': '80th_Percentile'
}

for df in [los_org_cmp, tpia_org_cmp, los_reg_cmp, tpia_reg_cmp]:
    for old_name, new_name in renamed_columns.items():
        df = df.withColumnRenamed(old_name, new_name)

def apply_conditions(df):
    conditions = [
        (F.col('PERCENTILE_90') < F.col('20th_Percentile')),
        (F.col('PERCENTILE_90') >= F.col('20th_Percentile')) & (F.col('PERCENTILE_90') <= F.col('80th_Percentile')),
        (F.col('PERCENTILE_90') > F.col('80th_Percentile'))
    ]

    values = ['001', '002', '003']
    descriptions = ['Above average performance', 'Same as average', 'Below average performance']

    # Apply conditions
    df = df.withColumn('COMPARE_IND_CODE', F.when(conditions[0], values[0])
                       .when(conditions[1], values[1])
                       .when(conditions[2], values[2])
                       .otherwise(''))

    df = df.withColumn('COMPARE_IND_E_DESC', F.when(conditions[0], descriptions[0])
                       .when(conditions[1], descriptions[1])
                       .when(conditions[2], descriptions[2])
                       .otherwise(''))

    # Sort the DataFrame by CORP_ID or REGION_ID as needed
    if 'CORP_ID' in df.columns:
        df = df.orderBy('CORP_ID')
    elif 'REGION_ID' in df.columns:
        df = df.orderBy('REGION_ID')

# Apply conditions for each DataFrame
apply_conditions(los_org_cmp)
apply_conditions(tpia_org_cmp)
apply_conditions(los_reg_cmp)
apply_conditions(tpia_reg_cmp)

from functools import reduce
from pyspark.sql import DataFrame

# Define a list of dataframes for los_org
los_org_dfs = [
    los_org_20.select('CORP_ID', 'CORP_PEER'),
    los_org_21.select('CORP_ID'),
    los_org_22_a.select('CORP_ID')
]

# Define a list of dataframes for tpia_org
tpia_org_dfs = [
    tpia_org_20.select('CORP_ID', 'CORP_PEER'),
    tpia_org_21.select('CORP_ID'),
    tpia_org_22_a.select('CORP_ID')
]

# Define a list of dataframes for los_reg
los_reg_dfs = [
    los_reg_20.select('REGION_ID'),
    los_reg_21.select('REGION_ID'),
    los_reg_22_a.select('REGION_ID')
]

# Define a list of dataframes for tpia_reg
tpia_reg_dfs = [
    tpia_reg_20.select('REGION_ID'),
    tpia_reg_21.select('REGION_ID'),
    tpia_reg_22_a.select('REGION_ID')
]

# Function to perform successive inner joins on a list of dataframes
def successive_inner_join(dataframes):
    result_df = dataframes[0]
    for df in dataframes[1:]:
        common_columns = set(result_df.columns).intersection(set(df.columns))
        join_columns = [col for col in result_df.columns if col in common_columns]
        result_df = result_df.join(df, join_columns, 'inner')
    return result_df

# Perform the successive joins for los_org
los_org_3x3 = successive_inner_join(los_org_dfs)

# Perform the successive joins for tpia_org
tpia_org_3x3 = successive_inner_join(tpia_org_dfs)

# Perform the successive joins for los_reg
los_reg_3x3 = successive_inner_join(los_reg_dfs)

# Perform the successive joins for tpia_reg
tpia_reg_3x3 = successive_inner_join(tpia_reg_dfs)

los_org_3x3.shape()
                                                                                
(283, 2)
tpia_org_3x3.shape()
                                                                                
(217, 2)
tpia_org_22_a.write.csv('tpia_org_22_a_spark.csv', header = True, mode='overwrite')
los_org_22_a.write.csv('los_org_22_a_spark.csv', header = True, mode='overwrite')
                                                                                
# # Rename column
tpia_org_22_a = tpia_org_22.withColumnRenamed("SUBMISSION_FISCAL", "CORP_PEER")
tpia_org_22_a = tpia_org_22.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")
# Union the datasets using unionByName with allowMissingColumns=True
# tpia_org_22_a = tpia_org_22_a.unionByName(filtered_rows_tpia, allowMissingColumns=True)

# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.unionByName(los_org_21, allowMissingColumns=True).unionByName(los_org_22_a, allowMissingColumns=True)
      .join(los_org_3x3, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for Tpia Org
tpia_org_all_yr_b = (
    tpia_org_20.unionByName(tpia_org_21, allowMissingColumns=True).unionByName(tpia_org_22_a, allowMissingColumns=True)
      .join(tpia_org_3x3, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for Los Reg
los_reg_all_yr_b = (
    los_reg_20.unionByName(los_reg_21, allowMissingColumns=True).unionByName(los_reg_22_a, allowMissingColumns=True)
      .join(los_reg_3x3, on='REGION_ID', how='inner')
      .withColumnRenamed("SUBMISSION_FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for TPIA Reg
tpia_reg_all_yr_b = (
    tpia_reg_20.unionByName(tpia_reg_21, allowMissingColumns=True).unionByName(tpia_reg_22_a, allowMissingColumns=True)
      .join(tpia_reg_3x3, on='REGION_ID', how='inner')
      .withColumnRenamed("SUBMISSION_FISCAL_YEAR", "TIME")
)

Selection deleted
# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.unionByName(los_org_21, allowMissingColumns=True)
               .unionByName(los_org_22_a, allowMissingColumns=True)
               .join(los_org_3x3, on='CORP_ID', how='inner')
               .withColumnRenamed("FISCAL_YEAR", "TIME")
               .select('CORP_ID', 'TIME', 'PERCENTILE_90')  # Ensure CORP_ID is included
)

# # Convert columns to numeric, just in case they are not
# los_org_all_yr_b = los_org_all_yr_b.withColumn('PERCENTILE_90', F.col('PERCENTILE_90').cast('float')) \
#                                    .withColumn('TIME', F.col('TIME').cast('float'))

# # Drop rows where either 'PERCENTILE_90' or 'TIME' is NaN after the conversion
# los_org_all_yr_b = los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'])

# # Create a GroupBy object for the 'CORP_ID' variable
# grouped = los_org_all_yr_b.groupBy('CORP_ID')



los_org_all_yr_b.show()
[Stage 5569:>                                                       (0 + 1) / 1]
+-------------+------+
|PERCENTILE_90|  TIME|
+-------------+------+
|         92.7|2021.0|
|         55.0|2020.0|
|         90.1|2022.0|
|         51.9|2021.0|
|        39.07|2020.0|
|         57.9|2022.0|
|         61.2|2021.0|
|         37.1|2020.0|
|         78.1|2022.0|
|        17.93|2021.0|
|         15.3|2020.0|
|         17.6|2022.0|
|        14.22|2021.0|
|        12.67|2020.0|
|         13.8|2022.0|
|        38.02|2021.0|
|        18.24|2020.0|
|         44.0|2022.0|
|        10.78|2021.0|
|         9.31|2020.0|
+-------------+------+
only showing top 20 rows

                                                                                
tpia_org_all_yr_b.show()
[Stage 5485:=====================================>                  (2 + 1) / 3]
+-------+----+---------+------------------+----------------------+--------------------+---------+
|CORP_ID|TIME|CORP_PEER|     PERCENTILE_90|SUBMISSION_FISCAL_YEAR|           CORP_NAME|CORP_PEER|
+-------+----+---------+------------------+----------------------+--------------------+---------+
|  120.0|2020|       H2|              4.72|                  NULL|                NULL|       H2|
|  120.0|2021|       H2|              5.83|                  NULL|                NULL|       H2|
|  120.0|NULL|       H2|              7.55|                  2022|Prince County Hos...|       H2|
|  936.0|2020|       H3|               0.9|                  NULL|                NULL|       H3|
|  936.0|2021|       H3|              1.05|                  NULL|                NULL|       H3|
|  936.0|NULL|       H3|1.4000000000000001|                  2022|Bassano Health Ce...|       H3|
|  939.0|2020|       H3|               1.3|                  NULL|                NULL|       H3|
|  939.0|2021|       H3|              1.17|                  NULL|                NULL|       H3|
|  939.0|NULL|       H3|              1.27|                  2022|Big Country Hospital|       H3|
|  941.0|2020|       H3|              1.25|                  NULL|                NULL|       H3|
|  941.0|2021|       H3|              1.32|                  NULL|                NULL|       H3|
|  941.0|NULL|       H3|              1.45|                  2022|Bow Island Health...|       H3|
|  944.0|2020|       H3|              2.42|                  NULL|                NULL|       H3|
|  944.0|2021|       H3|               3.4|                  NULL|                NULL|       H3|
|  944.0|NULL|       H3|              3.93|                  2022|Brooks Health Centre|       H3|
|  945.0|2020|        T|              2.05|                  NULL|                NULL|        T|
|  945.0|2021|        T|              4.05|                  NULL|                NULL|        T|
|  945.0|NULL|        T|              5.72|                  2022|Alberta Children’...|        T|
|  946.0|2020|        T|              3.15|                  NULL|                NULL|        T|
|  946.0|2021|        T|              4.77|                  NULL|                NULL|        T|
+-------+----+---------+------------------+----------------------+--------------------+---------+
only showing top 20 rows

                                                                                
tpia_org_22.shape()
                                                                                
(334, 5)






Working on LinearRegression Below. Do Not Touch Code above as they are correct 






from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import FloatType, IntegerType

# # Convert columns to numeric and drop NaNs
# los_org_all_yr_b = los_org_all_yr_b.select(
#     *[F.col(col).cast("float") for col in ['PERCENTILE_90', 'TIME']]
# ).na.drop(subset=['PERCENTILE_90', 'TIME'])

# Define a window specification
windowSpec = Window.partitionBy('CORP_ID')

# Create a GroupBy object and prepare DataFrame for results
grouped = los_org_all_yr_b.groupBy('CORP_ID').agg(
    F.collect_list('TIME').alias('TIME_LIST'),
    F.collect_list('PERCENTILE_90').alias('PERCENTILE_90_LIST')
)

all_results = []

# Iterate, perform regression, and store results
for row in grouped.collect():
    corp_id = row['CORP_ID']
    X = list(zip(row['TIME_LIST'], [1]*len(row['TIME_LIST'])))
    y = row['PERCENTILE_90_LIST']

    # Create a DataFrame from the data
    df = spark.createDataFrame(X, ["TIME", "const"]).withColumn("PERCENTILE_90", F.lit(None))
    
    # VectorAssembler
    assembler = VectorAssembler(inputCols=["TIME", "const"], outputCol="features")
    df = assembler.transform(df)
    
    # Linear Regression
    lr = LinearRegression(featuresCol='features', labelCol='PERCENTILE_90', regParam=0.0)
    model = lr.fit(df)
    
    # Extract coefficients and confidence intervals
    params = model.coefficients.toArray()
    std_err = model.summary.coefficientStandardErrors
    t_values = model.summary.tValues
    p_values = model.summary.pValues
    conf_int = model.summary.confidenceIntervals()

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [params, std_err, t_values, p_values,
                                   [conf_int[0][0]], [conf_int[0][1]]]):
        all_results.append({'CORP_ID': corp_id, '_TYPE_': param_type, 'TIME': float(values[0])})

# Convert to DataFrame and analyze results
los_org_trend_a = spark.createDataFrame(all_results)

# Add linr column
los_org_trend_a = los_org_trend_a.withColumn(
    'linr', F.when((F.col('_TYPE_') == 'PVALUE') & (F.col('TIME') < 0.05), 1).otherwise(0)
)

# Create subsets and merge
p_val = los_org_trend_a.filter(los_org_trend_a['_TYPE_'] == 'PVALUE').select('CORP_ID', 'linr')
parms = los_org_trend_a.filter(los_org_trend_a['_TYPE_'] == 'PARAMS').select('CORP_ID', 'TIME')

merged = p_val.join(parms, on='CORP_ID', how='inner')

# Define improvement indicators
merged = merged.withColumn('IMPROVEMENT_IND_CODE', F.lit('002')) \
               .withColumn('IMPROVEMENT_IND_E_DESC', F.lit('No Change'))

merged = merged.withColumn(
    'IMPROVEMENT_IND_CODE', 
    F.when((F.col('linr') == 1) & (F.col('TIME') > 0), '003')
    .when((F.col('linr') == 1) & (F.col('TIME') < 0), '001')
    .otherwise(F.col('IMPROVEMENT_IND_CODE'))
)

merged = merged.withColumn(
    'IMPROVEMENT_IND_E_DESC', 
    F.when((F.col('linr') == 1) & (F.col('TIME') > 0), 'Weakening')
    .when((F.col('linr') == 1) & (F.col('TIME') < 0), 'Improving')
    .otherwise(F.col('IMPROVEMENT_IND_E_DESC'))
)

# Final filtering
los_org_trend_b = merged.join(ed_nacrs_flg_1_22, on='CORP_ID', how='left_anti')

                                                                                
---------------------------------------------------------------------------
IllegalArgumentException                  Traceback (most recent call last)
/tmp/ipykernel_3755/1755871630.py in <cell line: 24>()
     32     # VectorAssembler
     33     assembler = VectorAssembler(inputCols=["TIME", "const"], outputCol="features")
---> 34     df = assembler.transform(df)
     35 
     36     # Linear Regression

/usr/local/lib/python3.10/dist-packages/pyspark/ml/base.py in transform(self, dataset, params)
    260                 return self.copy(params)._transform(dataset)
    261             else:
--> 262                 return self._transform(dataset)
    263         else:
    264             raise TypeError("Params must be a param map but got %s." % type(params))

/usr/local/lib/python3.10/dist-packages/pyspark/ml/wrapper.py in _transform(self, dataset)
    396 
    397         self._transfer_params_to_java()
--> 398         return DataFrame(self._java_obj.transform(dataset._jdf), dataset.sparkSession)
    399 
    400 

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

IllegalArgumentException: Data type string of column TIME is not supported.
