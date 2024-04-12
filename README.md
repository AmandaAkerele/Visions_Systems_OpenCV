# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    pd.concat([los_org_20, los_org_21, los_org_22_a], ignore_index=True)
      .merge(los_org_3x3[['CORP_ID']], on='CORP_ID', how='inner')
      .rename(columns={'FISCAL_YEAR': 'TIME'})
)

# Concatenate, merge, and rename for Tpia Org
tpia_org_all_yr_b = (
    pd.concat([tpia_org_20, tpia_org_21, tpia_org_22_a], ignore_index= True)
      .merge(tpia_org_3x3[['CORP_ID']], on='CORP_ID', how='inner')
      .rename(columns={'FISCAL_YEAR': 'TIME'})
)


# Concatenate, merge and rename for los_reg
los_reg_all_yr_b = (
    pd.concat([los_reg_20, los_reg_21, los_reg_22_a], ignore_index=True)
      .merge(los_reg_3x3[['REGION_ID']], on='REGION_ID', how='inner')
      .rename(columns={'SUBMISSION_FISCAL_YEAR': 'TIME'})
)

# Concatenate, merge and rename for TPIA Reg
tpia_reg_all_yr_b = (
    pd.concat([tpia_reg_20, tpia_reg_21, tpia_reg_22_a], ignore_index=True)
      .merge(tpia_reg_3x3[['REGION_ID']], on='REGION_ID', how='inner')
      .rename(columns={'SUBMISSION_FISCAL_YEAR': 'TIME'})
)
