from pyspark.sql import functions as F

# Convert pandas DataFrames to PySpark DataFrames (assuming they are already available as such)
los_corp_spark = spark.createDataFrame(los_corp)
LOS_supp_org_spark = spark.createDataFrame(LOS_supp_org)
los_reg_spark = spark.createDataFrame(los_reg)
LOS_supp_reg_spark = spark.createDataFrame(LOS_supp_reg)
los_org_21_spark = spark.createDataFrame(los_org_21)
los_org_20_spark = spark.createDataFrame(los_org_20)
los_peer_base_spark = spark.createDataFrame(los_peer_base)
los_reg_base_spark = spark.createDataFrame(los_reg_base)

# Remove SUPP entries
remove_supp_los_corp = ~los_corp_spark['CORP_ID'].isin([row.CORP_ID for row in LOS_supp_org_spark.collect()])
los_org_22_spark = los_corp_spark.filter(remove_supp_los_corp)
los_org_22_spark = los_org_22_spark.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR') \
                                   .withColumnRenamed('percentile_90', 'PERCENTILE_90')
los_org_22_spark = los_org_22_spark.withColumn('CORP_ID', F.when(F.col('CORP_ID') == 5085, 81180) \
                                                           .when(F.col('CORP_ID') == 5049, 81263) \
                                                           .otherwise(F.col('CORP_ID')))

remove_supp_los_reg = ~los_reg_spark['NEW_REGION_ID'].isin([row.NEW_REGION_ID for row in LOS_supp_reg_spark.collect()])
los_reg_22_spark = los_reg_spark.filter(remove_supp_los_reg)
los_reg_22_spark = los_reg_22_spark.withColumnRenamed('NEW_REGION_ID', 'REGION_ID') \
                                   .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR') \
                                   .withColumnRenamed('percentile_90', 'PERCENTILE_90')

# Apply transformations to CORP_ID
corrections = {
    1019: 81170,
    10038: 81124,
    7077: 80960,
    5045: 81131,
    5085: 81180,
    5049: 81263
}

for col in ['CORP_ID', 'PEER_GROUP_ID']:
    los_org_21_spark = los_org_21_spark.withColumn(col, F.when(F.col(col).isin(list(corrections.keys())), 
                                                               F.create_map([F.lit(x) for x in corrections.keys()], 
                                                                            [F.lit(x) for x in corrections.values()])[F.col(col)]).otherwise(F.col(col)))
los_org_21_spark = los_org_21_spark.filter(F.col('CORP_ID') != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

for col in ['CORP_ID', 'PEER_GROUP_ID']:
    los_org_20_spark = los_org_20_spark.withColumn(col, F.when(F.col(col).isin(list(corrections.keys())), 
                                                               F.create_map([F.lit(x) for x in corrections.keys()], 
                                                                            [F.lit(x) for x in corrections.values()])[F.col(col)]).otherwise(F.col(col)))
los_org_20_spark = los_org_20_spark.filter(F.col('CORP_ID') != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

los_org_22_spark = los_org_22_spark.filter(F.col('CORP_ID') != 5160).withColumnRenamed('percentile_90', 'PERCENTILE_90')

# Merge operations
los_org_cmp_spark = los_org_22_spark.join(los_peer_base_spark, on='CORP_PEER', how='inner')
los_reg_cmp_spark = los_reg_22_spark.join(los_reg_base_spark, los_reg_22_spark['FISCAL_YEAR'] == los_reg_base_spark['SUBMISSION_FISCAL_YEAR'], how='inner')
