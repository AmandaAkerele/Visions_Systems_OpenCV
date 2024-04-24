convert this code below using pyspark 



los_org_3x3_a= pd.merge(los_org_20[['CORP_ID', 'CORP_PEER']], los_org_21[['CORP_ID']], on='CORP_ID', how='inner')
los_org_3x3 = pd.merge(los_org_3x3_a, los_org_22[['CORP_ID']], on='CORP_ID', how='inner')
los_reg_3x3_a= pd.merge(los_reg_20[['REGION_ID']], los_reg_21[['REGION_ID']], on='REGION_ID', how='inner')
los_reg_3x3 = pd.merge(los_reg_3x3_a, los_reg_22[['REGION_ID']], on='REGION_ID', how='inner')

los_org_all_yr=pd.concat([los_org_20,los_org_21,los_org_22],ignore_index=True)
los_reg_all_yr=pd.concat([los_reg_20,los_reg_21,los_reg_22],ignore_index=True)

los_org_all_yr_a= pd.merge(los_org_all_yr, los_org_3x3[['CORP_ID']], on='CORP_ID', how='inner')
los_reg_all_yr_a= pd.merge(los_reg_all_yr, los_reg_3x3[['REGION_ID']], on='REGION_ID', how='inner')

los_org_all_yr_b = los_org_all_yr_a.rename(columns={'FISCAL_YEAR': 'TIME'})
los_reg_all_yr_b = los_reg_all_yr_a.rename(columns={'FISCAL_YEAR': 'TIME'})



tpia_org_3x3_a= pd.merge(tpia_org_20[['CORP_ID', 'CORP_PEER']], tpia_org_21[['CORP_ID']], on='CORP_ID', how='inner')
tpia_org_3x3 = pd.merge(tpia_org_3x3_a, tpia_org_22[['CORP_ID']], on='CORP_ID', how='inner')
tpia_reg_3x3_a= pd.merge(tpia_reg_20[['REGION_ID']], tpia_reg_21[['REGION_ID']], on='REGION_ID', how='inner')
tpia_reg_3x3 = pd.merge(tpia_reg_3x3_a, tpia_reg_22[['REGION_ID']], on='REGION_ID', how='inner')

tpia_org_all_yr=pd.concat([tpia_org_20,tpia_org_21,tpia_org_22],ignore_index=True)
tpia_reg_all_yr=pd.concat([tpia_reg_20,tpia_reg_21,tpia_reg_22],ignore_index=True)

tpia_org_all_yr_a= pd.merge(tpia_org_all_yr, tpia_org_3x3[['CORP_ID']], on='CORP_ID', how='inner')
tpia_reg_all_yr_a= pd.merge(tpia_reg_all_yr, tpia_reg_3x3[['REGION_ID']], on='REGION_ID', how='inner')

tpia_org_all_yr_b = tpia_org_all_yr_a.rename(columns={'FISCAL_YEAR': 'TIME'})
tpia_reg_all_yr_b = tpia_reg_all_yr_a.rename(columns={'FISCAL_YEAR': 'TIME'})
