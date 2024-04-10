from pyspark.sql import functions as F

# Transformations for LOS
remove_supp_los = los_org_22.join(los_supp_org_22, on='CORP_ID', how='left_anti')
los_org_22_a = remove_supp_los.select(
    F.col('SUBMISSION_FISCAL_YEAR').alias('FISCAL_YEAR'),
    F.col('percentile_90').alias('PERCENTILE_90')
)
los_org_22_a = los_org_22_a.withColumn('CORP_ID', 
                                   F.when(F.col('CORP_ID') == 5085, 81180)
                                   .when(F.col('CORP_ID') == 5049, 81263)
                                   .otherwise(F.col('CORP_ID')))

corrections = {
    1019: 81170,
    10038: 81124,
    7077: 80960,
    5045: 81131,
    5085: 81180,
    5049: 81263
}

for original, corrected in corrections.items():
    los_org_21 = los_org_21.withColumn('CORP_ID', 
                                       F.when(F.col('CORP_ID') == original, corrected)
                                       .otherwise(F.col('CORP_ID')))

los_org_21 = los_org_21.filter(F.col('CORP_ID') != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

for original, corrected in corrections.items():
    los_org_20 = los_org_20.withColumn('CORP_ID', 
                                       F.when(F.col('CORP_ID') == original, corrected)
                                       .otherwise(F.col('CORP_ID')))

los_org_20 = los_org_20.filter(F.col('CORP_ID') != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
los_org_22_a = los_org_22_a.filter(F.col('CORP_ID') != 5160).withColumnRenamed('percentile_90', 'PERCENTILE_90')

los_org_cmp = los_org_22_a.join(los_peer_base, on='CORP_PEER')
los_reg_cmp = los_org_22_a.join(los_reg_base, on=F.col('FISCAL_YEAR') == F.col('SUBMISSION_FISCAL_YEAR'))

# Transformations for TPIA
remove_supp_tpia = tpia_org_22.join(tpia_supp_org_22, on='CORP_ID', how='left_anti')
tpia_org_22_a = remove_supp_tpia.select(
    F.col('SUBMISSION_FISCAL_YEAR').alias('FISCAL_YEAR'),
    F.col('percentile_90').alias('PERCENTILE_90')
)
tpia_org_22_a = tpia_org_22_a.withColumn('CORP_ID', 
                                     F.when(F.col('CORP_ID') == 5085, 81180)
                                     .when(F.col('CORP_ID') == 5049, 81263)
                                     .otherwise(F.col('CORP_ID')))

for original, corrected in corrections.items():
    tpia_org_21 = tpia_org_21.withColumn('CORP_ID', 
                                         F.when(F.col('CORP_ID') == original, corrected)
                                         .otherwise(F.col('CORP_ID')))

tpia_org_21 = tpia_org_21.filter(F.col('CORP_ID') != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

for original, corrected in corrections.items():
    tpia_org_20 = tpia_org_20.withColumn('CORP_ID', 
                                         F.when(F.col('CORP_ID') == original, corrected)
                                         .otherwise(F.col('CORP_ID')))

tpia_org_20 = tpia_org_20.filter(F.col('CORP_ID') != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
tpia_org_22_a = tpia_org_22_a.filter(F.col('CORP_ID') != 5160).withColumnRenamed('percentile_90', 'PERCENTILE_90')

tpia_org_cmp = tpia_org_22_a.join(tpia_peer_base, on='CORP_PEER')
tpia_reg_cmp = tpia_org_22_a.join(tpia_reg_base, on=F.col('FISCAL_YEAR') == F.col('SUBMISSION_FISCAL_YEAR'))
