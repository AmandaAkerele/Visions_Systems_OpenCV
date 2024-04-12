

# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20_spark.union(los_org_21_spark).union(los_org_22_a_spark)
      .join(los_org_3x3_spark, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for Tpia Org
tpia_org_all_yr_b = (
    tpia_org_20_spark.union(tpia_org_21_spark).union(tpia_org_22_a_spark)
      .join(tpia_org_3x3_spark, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for Los Reg
los_reg_all_yr_b = (
    los_reg_20_spark.union(los_reg_21_spark).union(los_reg_22_a_spark)
      .join(los_reg_3x3_spark, on='REGION_ID', how='inner')
      .withColumnRenamed("SUBMISSION_FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for TPIA Reg
tpia_reg_all_yr_b = (
    tpia_reg_20_spark.union(tpia_reg_21_spark).union(tpia_reg_22_a_spark)
      .join(tpia_reg_3x3_spark, on='REGION_ID', how='inner')
      .withColumnRenamed("SUBMISSION_FISCAL_YEAR", "TIME")
)
