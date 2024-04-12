tpia_org_all_yr=pd.concat([tpia_org_20,tpia_org_21,tpia_org_22],ignore_index=True)
tpia_reg_all_yr=pd.concat([tpia_reg_20,tpia_reg_21,tpia_reg_22],ignore_index=True)

tpia_org_all_yr_a= pd.merge(tpia_org_all_yr, tpia_org_3x3[['CORP_ID']], on='CORP_ID', how='inner')
tpia_reg_all_yr_a= pd.merge(tpia_reg_all_yr, tpia_reg_3x3[['REGION_ID']], on='REGION_ID', how='inner')

tpia_org_all_yr_b = tpia_org_all_yr_a.rename(columns={'FISCAL_YEAR': 'TIME'})
tpia_reg_all_yr_b = tpia_reg_all_yr_a.rename(columns={'FISCAL_YEAR': 'TIME'})
