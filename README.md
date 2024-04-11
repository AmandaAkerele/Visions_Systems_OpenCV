from pyspark.sql import functions as F
from pyspark.sql.types import MapType, IntegerType, StringType

# Define mapping dictionary for CORP_ID
mapping_dict = {
    1019: 81170,
    10038: 81124,
    7077: 80960,
    5045: 81131,
    5085: 81180,
    5049: 81263,
    5160: None
}

# Create mapping expression
mapping_expr = F.create_map([F.lit(x) for x in mapping_dict.items()])

# Apply mapping to CORP_ID columns for tpia_org_21 and tpia_org_20
tpia_org_21 = tpia_org_21.withColumn('CORP_ID', mapping_expr.getItem(tpia_org_21['CORP_ID']))
tpia_org_20 = tpia_org_20.withColumn('CORP_ID', mapping_expr.getItem(tpia_org_20['CORP_ID']))

# Filter out rows where CORP_ID is 5160 for tpia_org_21 and tpia_org_20
tpia_org_21 = tpia_org_21.filter(tpia_org_21['CORP_ID'] != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')
tpia_org_20 = tpia_org_20.filter(tpia_org_20['CORP_ID'] != 5160).withColumnRenamed('PEER_GROUP_ID', 'CORP_PEER')

# Apply mapping to CORP_ID and REGION_ID columns for tpia_org_22 and tpia_reg_22
tpia_org_22 = tpia_org_22.withColumn('CORP_ID', F.when(tpia_org_22['CORP_ID'] == 5085, 81180)
                                      .when(tpia_org_22['CORP_ID'] == 5049, 81263)
                                      .otherwise(tpia_org_22['CORP_ID']))
tpia_reg_22 = tpia_reg_22.withColumn('REGION_ID', F.when(tpia_reg_22['NEW_REGION_ID'] == 5085, 81180)
                                        .when(tpia_reg_22['NEW_REGION_ID'] == 5049, 81263)
                                        .otherwise(tpia_reg_22['NEW_REGION_ID']))

# Filter out rows not in tpia_supp_org and tpia_supp_reg
remove_supp_org = ~tpia_corp['CORP_ID'].isin(tpia_supp_org['CORP_ID'])
tpia_org_22 = tpia_org_22.filter(remove_supp_org).withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR').withColumnRenamed('percentile_90', 'PERCENTILE_90')

remove_supp_reg = ~tpia_reg['NEW_REGION_ID'].isin(tpia_supp_reg['NEW_REGION_ID'])
tpia_reg_22 = tpia_reg_22.filter(remove_supp_reg).withColumnRenamed('NEW_REGION_ID', 'REGION_ID').withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'FISCAL_YEAR').withColumnRenamed('percentile_90', 'PERCENTILE_90')

# Merging with tpia_peer_base and tpia_reg_base
tpia_org_cmp = tpia_org_22.join(tpia_peer_base, on='CORP_PEER', how='inner')
tpia_reg_cmp = tpia_reg_22.join(tpia_reg_base, on='FISCAL_YEAR', how='inner')

# Define function to apply conditions
def apply_conditions(row):
    if row['PERCENTILE_90'] < row['20th_Percentile']:
        row['COMPARE_IND_CODE'] = '001'
        row['COMPARE_IND_E_DESC'] = 'Above average performance'
    elif row['PERCENTILE_90'] >= row['20th_Percentile'] and row['PERCENTILE_90'] <= row['80th_Percentile']:
        row['COMPARE_IND_CODE'] = '002'
        row['COMPARE_IND_E_DESC'] = 'Same as average'
    elif row['PERCENTILE_90'] > row['80th_Percentile']:
        row['COMPARE_IND_CODE'] = '003'
        row['COMPARE_IND_E_DESC'] = 'Below average performance'
    return row

# Apply conditions
tpia_org_cmp = tpia_org_cmp.rdd.map(apply_conditions).toDF()
tpia_reg_cmp = tpia_reg_cmp.rdd.map(apply_conditions).toDF()

# Sort dataframes
tpia_org_cmp = tpia_org_cmp.sort('CORP_ID')
tpia_reg_cmp = tpia_reg_cmp.sort('REGION_ID')

# Merge tpia_org dataframes
tpia_org_3x3_a = tpia_org_20.join(tpia_org_21, on='CORP_ID', how='inner').join(tpia_org_22, on='CORP_ID', how='inner')

# Merge tpia_reg dataframes
tpia_reg_3x3_a = tpia_reg_20.join(tpia_reg_21, on='REGION_ID', how='inner').join(tpia_reg_22, on='REGION_ID', how='inner')

# Concatenate tpia_org and tpia_reg dataframes
tpia_org_all_yr = tpia_org_20.union(tpia_org_21).union(tpia_org_22)
tpia_reg_all_yr = tpia_reg_20.union(tpia_reg_21).union(tpia_reg_22)

# Merge concatenated dataframes with 3x3 dataframes
tpia_org_all_yr_a = tpia_org_all_yr.join(tpia_org_3x3_a.select('CORP_ID'), on='CORP_ID', how='inner')
tpia_reg_all_yr_a = tpia_reg_all_yr.join(tpia_reg_3x3_a.select('REGION_ID'), on='REGION_ID', how='inner')

# Rename columns
tpia_org_all_yr_b = tpia_org_all_yr_a.withColumnRenamed('FISCAL_YEAR', 'TIME')
tpia_reg_all_yr_b = tpia_reg_all_yr_a.withColumnRenamed('FISCAL_YEAR', 'TIME')
