from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Tpia_Los_Transformations").getOrCreate()

# Convert pandas DataFrames to PySpark DataFrames (assuming tpia_org_22, tpia_supp_org, tpia_reg_22, tpia_supp_reg, 
# los_org_22, los_supp_org_22, los_reg_22, los_supp_reg_22, los_org_21, los_org_20, tpia_org_21, tpia_org_20 are 
# already available as PySpark DataFrames)
tpia_org_22_spark = spark.createDataFrame(tpia_org_22)
tpia_supp_org_spark = spark.createDataFrame(tpia_supp_org)
tpia_reg_22_spark = spark.createDataFrame(tpia_reg_22)
tpia_supp_reg_spark = spark.createDataFrame(tpia_supp_reg)
los_org_22_spark = spark.createDataFrame(los_org_22)
los_supp_org_22_spark = spark.createDataFrame(los_supp_org_22)
los_reg_22_spark = spark.createDataFrame(los_reg_22)
los_supp_reg_22_spark = spark.createDataFrame(los_supp_reg_22)
los_org_21_spark = spark.createDataFrame(los_org_21)
los_org_20_spark = spark.createDataFrame(los_org_20)
tpia_org_21_spark = spark.createDataFrame(tpia_org_21)
tpia_org_20_spark = spark.createDataFrame(tpia_org_20)

# For Tpia_org_22
tpia_org_22_a_spark = tpia_org_22_spark.join(tpia_supp_org_spark.select('CORP_ID'), on='CORP_ID', how='left_anti')
tpia_org_22_a_spark = tpia_org_22_a_spark.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR')
tpia_org_22_a_spark = tpia_org_22_a_spark.withColumn('CORP_ID', F.when(F.col('CORP_ID') == 5085, 81180) \
                                                           .when(F.col('CORP_ID') == 5049, 81263) \
                                                           .otherwise(F.col('CORP_ID')))

# Tpia_reg_22
tpia_reg_22_a_spark = tpia_reg_22_spark.join(tpia_supp_reg_spark.select('NEW_REGION_ID'), on='NEW_REGION_ID', how='left_anti')
tpia_reg_22_a_spark = tpia_reg_22_a_spark.withColumnRenamed('NEW_REGION_ID', 'REGION_ID')

# For Los_org_22
los_org_22_a_spark = los_org_22_spark.join(los_supp_org_22_spark.select('CORP_ID'), on='CORP_ID', how='left_anti')
los_org_22_a_spark = los_org_22_a_spark.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR')
los_org_22_a_spark = los_org_22_a_spark.withColumn('CORP_ID', F.when(F.col('CORP_ID') == 5085, 81180) \
                                                           .when(F.col('CORP_ID') == 5049, 81263) \
                                                           .otherwise(F.col('CORP_ID')))

# For Los_reg_22
los_reg_22_a_spark = los_reg_22_spark.join(los_supp_reg_22_spark.select('NEW_REGION_ID'), on='NEW_REGION_ID', how='left_anti')
los_reg_22_a_spark = los_reg_22_a_spark.withColumnRenamed('NEW_REGION_ID', 'REGION_ID')

# Update data for specific CORP_ID values
corp_id_mapping = {
    1019: 81170,
    10038: 81124,
    7077: 80960,
    5045: 81131,
    5085: 81180,
    5049: 81263,
    5160: None
}

# Apply the mapping to CORP_ID columns in all DataFrames
dataframes = [los_org_21_spark, los_org_20_spark, los_org_22_a_spark, tpia_org_21_spark, tpia_org_20_spark, tpia_org_22_a_spark]
for df in dataframes:
    for col, val in corp_id_mapping.items():
        df = df.withColumn('CORP_ID', F.when(F.col('CORP_ID') == col, val).otherwise(F.col('CORP_ID')))

# Rename the 'PEER_GROUP_ID' column to 'CORP_PEER' in los_org_21_spark and los_org_20_spark &
# Rename the fiscal_year column to 'SUBMISSION_FISCAL_YEAR in los_reg_21_spark and tpia_reg_21_spark
los_org_21_spark = los_org_21_spark.withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
los_org_20_spark = los_org_20_spark.withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
tpia_org_21_spark = tpia_org_21_spark.withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
tpia_org_20_spark = tpia_org_20_spark.withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

# los_reg_21_spark = los_reg_21_spark.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR')
# tpia_reg_21_spark = tpia_reg_21_spark.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR')

# Filter out rows where CORP_ID is 5160
los_org_21_spark = los_org_21_spark.filter(F.col('CORP_ID') != 5160)
los_org_20_spark = los_org_20_spark.filter(F.col('CORP_ID') != 5160)

# Merge los_org_22_a_spark with los_peer_base_spark on 'CORP_PEER' column
los_org_cmp_spark = los_org_22_a_spark.join(los_peer_base_spark, on='CORP_PEER', how='inner')

# Merge tpia_org_22_a_spark with tpia_peer_base_spark on 'CORP_PEER' column
tpia_org_cmp_spark = tpia_org_22_a_spark.join(tpia_peer_base_spark, on='CORP_PEER', how='inner')

# Merge los_reg_22_a_spark with los_reg_base_spark on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
los_reg_cmp_spark = los_reg_22_a_spark.join(los_reg_base_spark, on='SUBMISSION_FISCAL_YEAR', how='inner')

# Merge tpia_reg_22_spark with tpia_reg_base_spark on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
tpia_reg_cmp_spark = tpia_reg_22_a_spark.join(tpia_reg_base_spark, on='SUBMISSION_FISCAL_YEAR', how='inner')

# Stop Spark session
spark.stop()
