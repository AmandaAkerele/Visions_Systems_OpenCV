from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Merge_and_Transform") \
    .getOrCreate()

# Merge dataframes on specified columns and suffixes
merged_df = df_fac.join(df_dq, 
                        on=['FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR'], 
                        how='inner') \
                   .withColumnRenamed('CORP_ID', 'CORP_ID_df_fac') \
                   .withColumnRenamed('REGION_ID', 'REGION_ID_df_fac') \
                   .withColumnRenamed('PROVINCE_ID', 'PROVINCE_ID_df_fac') \
                   .withColumnRenamed('NACRS_ED_FLG', 'NACRS_ED_FLG_df_fac') \
                   .withColumnRenamed('FISCAL_YEAR', 'FISCAL_YEAR_df_dq') \
                   .withColumnRenamed('CORP_ID', 'CORP_ID_df_dq') \
                   .withColumnRenamed('REGION_ID', 'REGION_ID_df_dq') \
                   .withColumnRenamed('PROVINCE_ID', 'PROVINCE_ID_df_dq') \
                   .withColumnRenamed('NACRS_ED_FLG', 'NACRS_ED_FLG_df_dq')

# Define a list of columns to keep
columns_to_keep = [
    'FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'CORP_ID_df_fac',
    'REGION_ID_df_fac', 'PROVINCE_ID_df_fac', 'NACRS_ED_FLG_df_fac'
]

# Add 'TYPE' column with values 'DQ' for merged_df
merged_df = merged_df.withColumn('TYPE', lit('DQ'))

# Create DataFrames t3 and t4 based on conditions
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1) \
           .select(columns_to_keep) \
           .withColumn('TYPE', lit('SL')) \
           .withColumn('IND', lit(''))

ps = df_ps.filter(df_ps['FISCAL_YEAR'].cast('string') == '2022')
t4 = df_fac.join(ps, df_fac['FACILITY_AM_CARE_NUM'] == ps['FACILITY_AM_CARE_NUM']) \
           .select(columns_to_keep) \
           .withColumn('TYPE', lit('PS')) \
           .withColumn('IND', lit(''))

# Union DataFrames t3, t4, and merged_df
result_df = t3.union(t4).union(merged_df)

# Remove duplicated columns and reset index
result_df = result_df.dropDuplicates() \
                     .select(*[col for col in result_df.columns if not col.startswith('CORP_ID_df_fac')]) \
                     .select(*[col for col in result_df.columns if not col.startswith('CORP_ID_df_dq')]) \
                     .orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Group by CORP_ID and count
filtered_df_fac = df_fac.join(result_df, 'CORP_ID', 'inner')
tmp_cnt_ed_facility_org = filtered_df_fac.groupBy('CORP_ID').count().withColumnRenamed('count', 'CORP_CNT')

# Merge result_df with tmp_cnt_ed_facility_org
ed_facility_org = result_df.join(tmp_cnt_ed_facility_org, 'CORP_ID', 'inner')

# Specify the columns in the desired order
columns_to_keep = [
    'SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM', 'SITE_ID', 'CORP_ID', 'REGION_ID',
    'PROVINCE_ID', 'TYPE', 'IND', 'NACRS_ED_FLG', 'CORP_CNT'
]

# Select the desired columns from the DataFrame
ed_facility_org = ed_facility_org.select(columns_to_keep)

# Display the resulting DataFrame
# ed_facility_org.show()

# Stop Spark session
spark.stop()



or 
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Merge_and_Transform") \
    .getOrCreate()

# Merge dataframes on specified columns
merged_df = df_fac.join(df_dq, 
                        on=['FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR'], 
                        how='inner') \
                  .withColumnRenamed('CORP_ID', 'CORP_ID_df_fac') \
                  .withColumnRenamed('REGION_ID', 'REGION_ID_df_fac') \
                  .withColumnRenamed('PROVINCE_ID', 'PROVINCE_ID_df_fac') \
                  .withColumnRenamed('NACRS_ED_FLG', 'NACRS_ED_FLG_df_fac') \
                  .withColumnRenamed('FISCAL_YEAR', 'FISCAL_YEAR_df_dq') \
                  .withColumnRenamed('CORP_ID', 'CORP_ID_df_dq') \
                  .withColumnRenamed('REGION_ID', 'REGION_ID_df_dq') \
                  .withColumnRenamed('PROVINCE_ID', 'PROVINCE_ID_df_dq') \
                  .withColumnRenamed('NACRS_ED_FLG', 'NACRS_ED_FLG_df_dq') \
                  .withColumn('TYPE', lit('DQ'))

# Filter df_fac based on NACRS_ED_FLG
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1) \
           .select('FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG') \
           .withColumn('TYPE', lit('SL')) \
           .withColumn('IND', lit(''))

# Filter df_fac based on PS criteria
ps = df_ps.filter(df_ps['FISCAL_YEAR'].cast('string') == '2022')
t4 = df_fac.join(ps, df_fac['FACILITY_AM_CARE_NUM'] == ps['FACILITY_AM_CARE_NUM']) \
           .select('FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG') \
           .withColumn('TYPE', lit('PS')) \
           .withColumn('IND', lit(''))

# Union DataFrames t3, t4, and merged_df
result_df = t3.union(t4).union(merged_df)

# Remove duplicated columns and reset index
result_df = result_df.dropDuplicates(['FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR']) \
                     .orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Filter df_fac based on CORP_ID
filtered_df_fac = df_fac.join(result_df.select('CORP_ID').distinct(), 'CORP_ID')

# Group by CORP_ID and count
tmp_cnt_ed_facility_org = filtered_df_fac.groupBy('CORP_ID').count().withColumnRenamed('count', 'CORP_CNT')

# Merge result_df with tmp_cnt_ed_facility_org
ed_facility_org = result_df.join(tmp_cnt_ed_facility_org, 'CORP_ID')

# Specify the columns in the desired order
columns_to_keep = [
    'SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM', 'SITE_ID', 'CORP_ID', 'REGION_ID',
    'PROVINCE_ID', 'TYPE', 'IND', 'NACRS_ED_FLG', 'CORP_CNT'
]

# Select the desired columns from the DataFrame
ed_facility_org = ed_facility_org.select(columns_to_keep)

# Display the resulting DataFrame
# ed_facility_org.show()

# Stop Spark session
spark.stop()


