please convert the code to pyspakr below. Ensure accuracy in calculation and ensure the row counts are correct 

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


 

