from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Multiple Sites Analysis") \
    .getOrCreate()

# Assuming df_fac and ed_facility_org are already defined DataFrames

# Create standalone DataFrame
stand_alone = df_fac.filter(col('NACRS_ED_FLG') == 1).select('CORP_ID')

# Create multiple_sites DataFrame
multiple_sites = df_fac.join(stand_alone, 'CORP_ID', 'inner') \
    .groupBy('CORP_ID').agg(count('*').alias('cnt_sites')) \
    .filter(col('cnt_sites') > 1)

# Get nacrs_ed_flg=1
ed_nacrs_flg_1_22 = df_fac.join(
    ed_facility_org.filter(
        (col('TYPE') == 'SL') & (col('CORP_CNT') == 1)
    ),
    'CORP_ID'
).filter(df_fac['NACRS_ED_FLG'] == 1)

# Stop Spark session
spark.stop()
