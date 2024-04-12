# Union the datasets using unionByName with allowMissingColumns=True
tpia_org_22 = tpia_org_22.unionByName(filtered_rows_tpia, allowMissingColumns=True)

# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.unionByName(los_org_21, allowMissingColumns=True).unionByName(los_org_22_a, allowMissingColumns=True)
      .join(los_org_3x3_spark, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for Tpia Org
tpia_org_all_yr_b = (
    tpia_org_20.unionByName(tpia_org_21, allowMissingColumns=True).unionByName(tpia_org_22_a, allowMissingColumns=True)
      .join(tpia_org_3x3_spark, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for Los Reg
los_reg_all_yr_b = (
    los_reg_20.unionByName(los_reg_21, allowMissingColumns=True).unionByName(los_reg_22_a, allowMissingColumns=True)
      .join(los_reg_3x3_spark, on='REGION_ID', how='inner')
      .withColumnRenamed("SUBMISSION_FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for TPIA Reg
tpia_reg_all_yr_b = (
    tpia_reg_20.unionByName(tpia_reg_21, allowMissingColumns=True).unionByName(tpia_reg_22_a, allowMissingColumns=True)
      .join(tpia_reg_3x3_spark, on='REGION_ID', how='inner')
      .withColumnRenamed("SUBMISSION_FISCAL_YEAR", "TIME")
)
