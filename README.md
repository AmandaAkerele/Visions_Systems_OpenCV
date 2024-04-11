from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Filter out rows not in tpia_supp_org for tpia_org_22
remove_supp = ~tpia_org_22.select('CORP_ID').isin(tpia_supp_org.select('CORP_ID')).alias('remove')
tpia_org_22_a = tpia_org_22.join(remove_supp, on='CORP_ID', how='left_anti') \
                      .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR') \
                      .withColumnRenamed('percentile_90', 'PERCENTILE_90')

tpia_org_22_a = tpia_org_22_a.withColumn('CORP_ID', 
                                     F.when(F.col('CORP_ID') == 5085, 81180)
                                      .when(F.col('CORP_ID') == 5049, 81263)
                                      .otherwise(F.col('CORP_ID')))

# Filter out rows not in tpia_supp_reg for tpia_reg_22
remove_supp = ~tpia_reg_22.select('NEW_REGION_ID').isin(tpia_supp_reg.select('NEW_REGION_ID')).alias('remove')
tpia_reg_22_a = tpia_reg_22.join(remove_supp, on='NEW_REGION_ID', how='left_anti') \
                      .withColumnRenamed('NEW_REGION_ID', 'REGION_ID') \
                      .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR') \
                      .withColumnRenamed('percentile_90', 'PERCENTILE_90')

# Update CORP_ID values for tpia_org_21 and tpia_org_20
corp_id_mapping = {
    1019: 81170,
    10038: 81124,
    7077: 80960,
    5045: 81131,
    5085: 81180,
    5049: 81263,
    5160: None
}

dataframes = [tpia_org_21, tpia_org_20]
for df in dataframes:
    for k, v in corp_id_mapping.items():
        df = df.withColumn('CORP_ID', F.when(F.col('CORP_ID') == k, v).otherwise(F.col('CORP_ID')))
    df = df.filter(F.col('CORP_ID') != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

# Merge tpia_org_22 with tpia_peer_base on 'CORP_PEER' column
tpia_org_cmp = tpia_org_22_a.join(tpia_peer_base, on='CORP_PEER', how='inner')

# Merge tpia_reg_22 with tpia_reg_base on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
tpia_reg_cmp = tpia_reg_22_a.join(tpia_reg_base, (tpia_reg_22_a['FISCAL_YEAR'] == tpia_reg_base['SUBMISSION_FISCAL_YEAR']), how='inner')

# Define the apply_conditions UDF
@F.udf(returnType=StructType([
    StructField("COMPARE_IND_CODE", StringType(), True),
    StructField("COMPARE_IND_E_DESC", StringType(), True)
]))
def apply_conditions(percentile_90, percentile_20, percentile_80):
    if percentile_90 < percentile_20:
        return ('001', 'Above average performance')
    elif percentile_90 >= percentile_20 and percentile_90 <= percentile_80:
        return ('002', 'Same as average')
    elif percentile_90 > percentile_80:
        return ('003', 'Below average performance')

# Apply conditions for tpia_org_cmp and tpia_reg_cmp
tpia_org_cmp_a = tpia_org_cmp.withColumn('temp', apply_conditions(F.col('PERCENTILE_90'), F.col('20th_Percentile'), F.col('80th_Percentile'))) \
                            .withColumn('COMPARE_IND_CODE', F.col('temp').getItem(0)) \
                            .withColumn('COMPARE_IND_E_DESC', F.col('temp').getItem(1)) \
                            .drop('temp') \
                            .orderBy('CORP_ID')

tpia_reg_cmp_a = tpia_reg_cmp.withColumn('temp', apply_conditions(F.col('PERCENTILE_90'), F.col('20th_Percentile'), F.col('80th_Percentile'))) \
                            .withColumn('COMPARE_IND_CODE', F.col('temp').getItem(0)) \
                            .withColumn('COMPARE_IND_E_DESC', F.col('temp').getItem(1)) \
                            .drop('temp') \
                            .orderBy('REGION_ID')

# Successive joins for tpia_org_3x3 and tpia_reg_3x3
tpia_org_3x3_a = tpia_org_20.select('CORP_ID', 'CORP_PEER').join(tpia_org_21.select('CORP_ID'), on='CORP_ID', how='inner') \
                         .join(tpia_org_22_a.select('CORP_ID'), on='CORP_ID', how='inner')

tpia_reg_3x3_a = tpia_reg_20.select('REGION_ID').join(tpia_reg_21.select('REGION_ID'), on='REGION_ID', how='inner') \
                         .join(tpia_reg_22_a.select('REGION_ID'), on='REGION_ID', how='inner')

# Concatenate tpia_org_20, tpia_org_21, tpia_org_22 and tpia_reg_20, tpia_reg_21, tpia_reg_22
tpia_org_all_yr = tpia_org_20.union(tpia_org_21).union(tpia_org_22_a)
tpia_reg_all_yr = tpia_reg_20.union(tpia_reg_21).union(tpia_reg_22_a)

# Successive joins for tpia_org_all_yr_a and tpia_reg_all_yr_a
tpia_org_all_yr_a = tpia_org_all_yr.join(tpia_org_3x3_a.select('CORP_ID'), on='CORP_ID', how='inner')
tpia_reg_all_yr_a = tpia_reg_all_yr.join(tpia_reg_3x3_a.select('REGION_ID'), on='REGION_ID', how='inner')

# Rename columns for tpia_org_all_yr_b and tpia_reg_all_yr_b
tpia_org_all_yr_b = tpia_org_all_yr_a.withColumnRenamed('FISCAL_YEAR', 'TIME')
tpia_reg_all_yr_b = tpia_reg_all_yr_a.withColumnRenamed('FISCAL_YEAR', 'TIME')
