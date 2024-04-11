remove_supp= ~tpia_corp['CORP_ID'].isin(tpia_supp_org['CORP_ID'])
tpia_org_22 = tpia_corp[remove_supp].rename(columns={'SUBMISSION_FISCAL_YEAR':'FISCAL_YEAR','percentile_90':'PERCENTILE_90'})
tpia_org_22['CORP_ID']=tpia_org_22['CORP_ID'].apply(lambda x: 81180 if x==5085 else x)
tpia_org_22['CORP_ID']=tpia_org_22['CORP_ID'].apply(lambda x: 81263 if x==5049 else x)
remove_supp= ~tpia_reg['NEW_REGION_ID'].isin(tpia_supp_reg['NEW_REGION_ID'])
tpia_reg_22 = tpia_reg[remove_supp].rename(columns={'NEW_REGION_ID': 'REGION_ID', 'SUBMISSION_FISCAL_YEAR':'FISCAL_YEAR','percentile_90':'PERCENTILE_90'})


tpia_org_21['CORP_ID']=tpia_org_21['CORP_ID'].apply(lambda x: 81170 if x==1019 else x)
tpia_org_21['CORP_ID']=tpia_org_21['CORP_ID'].apply(lambda x: 81124 if x==10038 else x)
tpia_org_21['CORP_ID']=tpia_org_21['CORP_ID'].apply(lambda x: 80960 if x==7077 else x)
tpia_org_21['CORP_ID']=tpia_org_21['CORP_ID'].apply(lambda x: 81131 if x==5045 else x)
tpia_org_21['CORP_ID']=tpia_org_21['CORP_ID'].apply(lambda x: 81180 if x==5085 else x)
tpia_org_21['CORP_ID']=tpia_org_21['CORP_ID'].apply(lambda x: 81263 if x==5049 else x)
tpia_org_21=tpia_org_21[tpia_org_21['CORP_ID'] != 5160].rename(columns={'PEER_GROUP_ID':'CORP_PEER'})

tpia_org_20['CORP_ID']=tpia_org_20['CORP_ID'].apply(lambda x: 81170 if x==1019 else x)
tpia_org_20['CORP_ID']=tpia_org_20['CORP_ID'].apply(lambda x: 81124 if x==10038 else x)
tpia_org_20['CORP_ID']=tpia_org_20['CORP_ID'].apply(lambda x: 80960 if x==7077 else x)
tpia_org_20['CORP_ID']=tpia_org_20['CORP_ID'].apply(lambda x: 81131 if x==5045 else x)
tpia_org_20['CORP_ID']=tpia_org_20['CORP_ID'].apply(lambda x: 81180 if x==5085 else x)
tpia_org_20['CORP_ID']=tpia_org_20['CORP_ID'].apply(lambda x: 81263 if x==5049 else x)
tpia_org_20=tpia_org_20[tpia_org_20['CORP_ID'] != 5160].rename(columns={'PEER_GROUP_ID':'CORP_PEER'})
tpia_org_22=tpia_org_22[tpia_org_22['CORP_ID'] != 5160].rename(columns={'percentile_90':'PERCENTILE_90'})

tpia_org_cmp= pd.merge(tpia_org_22, tpia_peer_base, on='CORP_PEER')
tpia_reg_cmp= pd.merge(tpia_reg_22, tpia_reg_base, left_on='FISCAL_YEAR', right_on='SUBMISSION_FISCAL_YEAR')




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

tpia_org_cmp_a=tpia_org_cmp.apply(apply_conditions, axis=1)
tpia_org_cmp_a = tpia_org_cmp_a.sort_values(by=['CORP_ID'])
tpia_reg_cmp_a=tpia_reg_cmp.apply(apply_conditions, axis=1)
tpia_reg_cmp_a = tpia_reg_cmp_a.sort_values(by=['REGION_ID'])

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
