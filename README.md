from pyspark.sql import functions as F

# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.union(los_org_21).union(los_org_22_a)
    .join(los_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
)

# Concatenate, merge, and rename for Tpia Org
tpia_org_all_yr_b = (
    tpia_org_20.union(tpia_org_21).union(tpia_org_22_a)
    .join(tpia_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
)

# Concatenate, merge and rename for los_reg
los_reg_all_yr_b = (
    los_reg_20.union(los_reg_21).union(los_reg_22_a)
    .join(los_reg_3x3.select('REGION_ID'), on='REGION_ID', how='inner')
    .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
)

# Concatenate, merge and rename for TPIA Reg
tpia_reg_all_yr_b = (
    tpia_reg_20.union(tpia_reg_21).union(tpia_reg_22_a)
    .join(tpia_reg_3x3.select('REGION_ID'), on='REGION_ID', how='inner')
    .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
)
