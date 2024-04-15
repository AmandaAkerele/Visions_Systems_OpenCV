# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.unionByName(los_org_21, allowMissingColumns=True).unionByName(los_org_22_a, allowMissingColumns=True)
      .join(los_org_3x3, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)


ensure CORP_ID is showing when calling los_org_all_yr_b dataframe as a column
