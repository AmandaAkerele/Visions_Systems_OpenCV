# Concatenate tpia_org DataFrames
tpia_org_all_yr = tpia_org_20.unionByName(tpia_org_21).unionByName(tpia_org_22).dropDuplicates()

# Concatenate tpia_reg DataFrames
tpia_reg_all_yr = tpia_reg_20.unionByName(tpia_reg_21).unionByName(tpia_reg_22).dropDuplicates()

# Inner join with tpia_org_3x3
tpia_org_all_yr_a = tpia_org_all_yr.join(tpia_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')

# Inner join with tpia_reg_3x3
tpia_reg_all_yr_a = tpia_reg_all_yr.join(tpia_reg_3x3.select('REGION_ID'), on='REGION_ID', how='inner')

# Rename columns
tpia_org_all_yr_b = tpia_org_all_yr_a.withColumnRenamed('FISCAL_YEAR', 'TIME')
tpia_reg_all_yr_b = tpia_reg_all_yr_a.withColumnRenamed('FISCAL_YEAR', 'TIME')
