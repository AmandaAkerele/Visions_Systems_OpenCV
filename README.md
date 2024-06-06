correctly convert the code below to pyspark 

Ensure accuracy in calcuation 
Also ensure accuracy in the count 

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
