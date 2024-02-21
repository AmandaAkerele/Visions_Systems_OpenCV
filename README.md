# For corp level without UCC
# Vectorized approach for 'tpia_rec' calculation
ed_record['tpia_rec'] = 'Y'
ed_record.loc[ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'].isna() | (ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'].str.strip() == ''), 'tpia_rec'] = 'B'
ed_record.loc[ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'tpia_rec'] = 'N'

# Filter records not equal to 'B'
tpia_org_cnt = ed_record[ed_record['tpia_rec'] != 'B']

# Aggregation
tpia_org_rec = tpia_org_cnt.groupby(['SUBMISSION_FISCAL_YEAR', 'CORP_ID']).agg(
    Total_CASE=('AM_CARE_KEY', 'count'),
    tpia_calc_cnt=('tpia_rec', lambda x: (x == 'Y').sum()),
    tpia_elig_cnt=('tpia_rec', lambda x: (x.isin(['Y', 'N'])).sum())
).reset_index()

# Calculate percentage and sort
tpia_org_rec['tpia_rec_pct'] = tpia_org_rec['tpia_calc_cnt'] / tpia_org_rec['Total_CASE']
tpia_org_rec.sort_values(by=['CORP_ID'], inplace=True)

# Conditional DataFrame creation
tpia_supp_org = tpia_org_rec.query("(tpia_calc_cnt < 50) or (tpia_calc_cnt > 50 and tpia_rec_pct < 0.75)")
tpia_rpt_org = tpia_org_rec.query("(tpia_calc_cnt >= 50) or (tpia_calc_cnt < 50 and tpia_rec_pct >= 0.75)")


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

# For corp level with ucc
# Create a Dataframe tpia_org_cnt_ucc_22_a
ed_record_with_ucc_22['tpia_rec'] = 'B'  
ed_record_with_ucc_22.loc[ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'tpia_rec'] = 'N'
ed_record_with_ucc_22.loc[ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'].notna() & 
                          (ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] != '9999'), 'tpia_rec'] = 'Y'

# Filter out rows with tpia_rec_22 equal to "B"
tpia_org_cnt_ucc_22 = ed_record_with_ucc_22[ed_record_with_ucc_22['tpia_rec'] != 'B']
ed_record_with_ucc_22[ed_record_with_ucc_22['tpia_rec'] != 'B']

tpia_org_rec_ucc_22 = tpia_org_cnt_ucc_22.groupby(['SUBMISSION_FISCAL_YEAR', 'CORP_ID']).agg(
    Total_CASE=pd.NamedAgg(column='AM_CARE_KEY', aggfunc='count'),
    tpia_calc_cnt=pd.NamedAgg(column='tpia_rec', aggfunc=lambda x: (x == 'Y').sum()),
    tpia_elig_cnt=pd.NamedAgg(column='tpia_rec', aggfunc=lambda x: (x.isin(['Y', 'N'])).sum()),
    tpia_rec_pct=pd.NamedAgg(column='tpia_rec', aggfunc=lambda x: (x == 'Y').sum() / len(x))
).reset_index()


# Filter 'tpia_org_rec_ucc' based on conditions
tpia_supp_org_ucc_22 = tpia_org_rec_ucc_22[(tpia_org_rec_ucc_22['tpia_calc_cnt']<50) | 
                                           ((tpia_org_rec_ucc_22['tpia_calc_cnt'] >50) & 
                                            (tpia_org_rec_ucc_22['tpia_rec_pct'] < 0.75))]

tpia_rpt_org_ucc_22 = tpia_org_rec_ucc_22[(tpia_org_rec_ucc_22['tpia_calc_cnt'] >=50) | 
                                          ((tpia_org_rec_ucc_22['tpia_calc_cnt'] < 50) & 
                                           (tpia_org_rec_ucc_22['tpia_rec_pct'] >=0.75))]


 
