tmp_ed_facility_org_a= pd.merge(df_fac, df_dq, how='inner',
                                      left_on=['FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR'],
                                      right_on=['FACILITY_AM_CARE_NUM', 'FISCAL_YEAR'])
# Rename columns without suffixes
column_mapping = {'SITE_ID_x': 'SITE_ID',
                  'CORP_ID_x': 'CORP_ID', 
                  'REGION_ID_x': 'REGION_ID',
                  'PROVINCE_ID_x': 'PROVINCE_ID',
                  'SITE_ID_y': 'SITE_ID',
                  'CORP_ID_y': 'CORP_ID', 
                  'REGION_ID_y': 'REGION_ID',
                  'PROVINCE_ID_y': 'PROVINCE_ID'}
tmp_ed_facility_org_a = tmp_ed_facility_org_a.rename(columns=column_mapping)
tmp_ed_facility_org_a= tmp_ed_facility_org_a.loc[:, ~tmp_ed_facility_org_a.columns.duplicated()]


tmp_ed_facility_org_a['TYPE'] = 'DQ'
t3 = df_fac[df_fac['NACRS_ED_FLG'] == 1][['SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM', 'SITE_ID', 'CORP_ID',
                                               'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG']]
t3.loc[:,'TYPE'] = 'SL'
t3.loc[:,'IND'] = ''
#t3['row_num']=range(1,len(t3)=1)

ps= df_ps[df_ps['FISCAL_YEAR'].astype(str)=='2022']
t4 = df_fac[df_fac['FACILITY_AM_CARE_NUM'].astype(str).isin(ps['FACILITY_AM_CARE_NUM'].astype(str))]

t4.loc[:,'TYPE'] = 'PS'
t4.loc[:,'IND'] = ''
tmp_ed_facility_org=pd.concat([tmp_ed_facility_org_a,t3,t4],ignore_index=True)
    # Drop duplicate columns
tmp_ed_facility_org= tmp_ed_facility_org.loc[:, ~tmp_ed_facility_org.columns.duplicated()]



filtered_ed_fac = df_fac[df_fac['CORP_ID'].isin(tmp_ed_facility_org['CORP_ID'])]

# Group by CORP_ID and count
tmp_cnt_ed_facility_org = filtered_ed_fac.groupby('CORP_ID').size().reset_index(name='CORP_CNT')

# Create ED_FACILITY_ORG
# Merge TMP_ED_FACILITY_ORG with TMP_CNT_ED_FACILITY_ORG
ed_facility_org = pd.merge(tmp_ed_facility_org, tmp_cnt_ed_facility_org, on='CORP_ID')

# Sort the DataFrame
ed_facility_org = ed_facility_org.sort_values(by=['TYPE', 'FACILITY_AM_CARE_NUM'])
ed_facility_org = ed_facility_org.rename(columns={'REGION_NAME_x' : 'REGION_NAME','REGION_NAME_y' : 'REGION_NAME'})
 # Drop duplicate columns
ed_facility_org= ed_facility_org.loc[:, ~ed_facility_org.columns.duplicated()]
