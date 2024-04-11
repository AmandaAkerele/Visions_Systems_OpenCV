# Concatenate, merge, and rename for Los Org and Tpia Org
los_org_all_yr_b = (
    los_org_20
    .union(los_org_21)
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
    .union(los_org_22_a.select(los_org_20.columns))  # Selecting the same columns as los_org_20 for los_org_22_a
    .join(los_org_3x3, on='CORP_ID', how='inner')
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
)


# Concatenate, merge, and rename for Los Org and Tpia Org
tpia_org_all_yr_b = (
    tpia_org_20
    .union(tpia_org_21)
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
    .union(tpia_org_22_a.select(tpia_org_20.columns))  # Selecting the same columns as tpia_org_20 for tpia_org_22_a
    .join(tpia_org_3x3, on='CORP_ID', how='inner')
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
)

# Concatenate, merge, and rename for Los Reg and Tpia Reg
tpia_reg_all_yr_b = (
    tpia_reg_20
    .union(tpia_reg_21)
    .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
    .union(tpia_reg_22_a.select(tpia_reg_20.columns))  # Selecting the same columns as tpia_reg_20 for tpia_reg_22_a
    .join(tpia_reg_3x3, on='REGION_ID', how='inner')
    .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
)
