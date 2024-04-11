from pyspark.sql import functions as F

# Remove rows from tpia_corp where CORP_ID is in tpia_supp_org
remove_supp = ~tpia_corp['CORP_ID'].isin(tpia_supp_org['CORP_ID'])
tpia_org_22 = tpia_corp.filter(remove_supp)
tpia_org_22 = tpia_org_22.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR') \
                         .withColumnRenamed('percentile_90', 'PERCENTILE_90')
tpia_org_22 = tpia_org_22.withColumn('CORP_ID', F.when(tpia_org_22['CORP_ID'] == 5085, 81180)
                                           .when(tpia_org_22['CORP_ID'] == 5049, 81263)
                                           .otherwise(tpia_org_22['CORP_ID']))

# Remove rows from tpia_reg where NEW_REGION_ID is in tpia_supp_reg
remove_supp = ~tpia_reg['NEW_REGION_ID'].isin(tpia_supp_reg['NEW_REGION_ID'])
tpia_reg_22 = tpia_reg.filter(remove_supp)
tpia_reg_22 = tpia_reg_22.withColumnRenamed('NEW_REGION_ID', 'REGION_ID') \
                         .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR') \
                         .withColumnRenamed('percentile_90', 'PERCENTILE_90')

# Apply CORP_ID mapping for tpia_org_21
mapping_expr = F.create_map([F.lit(x) for x in 
                             [(1019, 81170), (10038, 81124), (7077, 80960), 
                              (5045, 81131), (5085, 81180), (5049, 81263), (5160, None)]])
tpia_org_21 = tpia_org_21.withColumn('CORP_ID', mapping_expr.getItem(tpia_org_21['CORP_ID']))
tpia_org_21 = tpia_org_21.filter(tpia_org_21['CORP_ID'] != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

# Apply CORP_ID mapping for tpia_org_20
tpia_org_20 = tpia_org_20.withColumn('CORP_ID', mapping_expr.getItem(tpia_org_20['CORP_ID']))
tpia_org_20 = tpia_org_20.filter(tpia_org_20['CORP_ID'] != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

# Apply conditions using a UDF
def apply_conditions(percentile_90, percentile_20, percentile_80):
    if percentile_90 < percentile_20:
        return '001', 'Above average performance'
    elif percentile_90 >= percentile_20 and percentile_90 <= percentile_80:
        return '002', 'Same as average'
    else:
        return '003', 'Below average performance'

apply_conditions_udf = F.udf(apply_conditions, returnType='struct<COMPARE_IND_CODE:string, COMPARE_IND_E_DESC:string>')

tpia_org_cmp = tpia_org_22.join(tpia_peer_base, on='CORP_PEER')
tpia_org_cmp = tpia_org_cmp.withColumn('conditions', apply_conditions_udf('PERCENTILE_90', '20th_Percentile', '80th_Percentile')) \
                           .select('*', 'conditions.*').drop('conditions') \
                           .orderBy('CORP_ID')

tpia_reg_cmp = tpia_reg_22.join(tpia_reg_base, tpia_reg_22['FISCAL_YEAR'] == tpia_reg_base['SUBMISSION_FISCAL_YEAR'])
tpia_reg_cmp = tpia_reg_cmp.withColumn('conditions', apply_conditions_udf('PERCENTILE_90', '20th_Percentile', '80th_Percentile')) \
                           .select('*', 'conditions.*').drop('conditions') \
                           .orderBy('REGION_ID')

# Merge operations for tpia_org_3x3 and tpia_reg_3x3
tpia_org_3x3_a = tpia_org_20.select('CORP_ID', 'CORP_PEER').join(tpia_org_21.select('CORP_ID'), on='CORP_ID', how='inner') \
                         .join(tpia_org_22.select('CORP_ID'), on='CORP_ID', how='inner')

tpia_reg_3x3_a = tpia_reg_20.select('REGION_ID').join(tpia_reg_21.select('REGION_ID'), on='REGION_ID', how='inner') \
                         .join(tpia_reg_22.select('REGION_ID'), on='REGION_ID', how='inner')

# Concatenate all tpia_org and tpia_reg dataframes
tpia_org_all_yr = tpia_org_20.union(tpia_org_21).union(tpia_org_22)
tpia_reg_all_yr = tpia_reg_20.union(tpia_reg_21).union(tpia_reg_22)

# Merge operations for tpia_org_all_yr_a and tpia_reg_all_yr_a
tpia_org_all_yr_a = tpia_org_all_yr.join(tpia_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')
tpia_reg_all_yr_a = tpia_reg_all_yr.join(tpia_reg_3x3.select('REGION_ID'), on='REGION_ID', how='inner')

# Rename columns in tpia_org_all_yr_a and tpia_reg_all_yr_a
tpia_org_all_yr_b = tpia_org_all_yr_a.withColumnRenamed('FISCAL_YEAR', 'TIME')
tpia_reg_all_yr_b = tpia_reg_all_yr_a.withColumnRenamed('FISCAL_YEAR', 'TIME')
