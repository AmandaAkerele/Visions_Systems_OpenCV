# For Peer level

tpia_peer_cnt_a_df = ed_record_22_Peer.copy()
tpia_peer_cnt_a_df['tpia_rec'] = 'B'
tpia_peer_cnt_a_df.loc[tpia_peer_cnt_a_df['TIME_PHYSICAN_INIT_ASSESSMENT'].str.strip() == '', 'tpia_rec'] = 'B'
tpia_peer_cnt_a_df.loc[tpia_peer_cnt_a_df['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'tpia_rec'] = 'N'
tpia_peer_cnt_a_df.loc[~tpia_peer_cnt_a_df['TIME_PHYSICAN_INIT_ASSESSMENT'].isin(['9999', '']), 'tpia_rec'] = 'Y'

tpia_peer_cnt_df = tpia_peer_cnt_a_df[tpia_peer_cnt_a_df['tpia_rec'] !='B']

tpia_peer_rec_df = tpia_peer_cnt_df.groupby(['SUBMISSION_FISCAL_YEAR', 'SITE_PEER']).agg(
    tpia_calc_cnt = pd.NamedAgg(column='tpia_rec', aggfunc=lambda x: (x == 'Y').sum()),
    tpia_elig_cnt= pd.NamedAgg(column='tpia_rec', aggfunc=lambda x: (x.isin(['Y', 'N'])).sum()),
    tpia_rec_pct=pd.NamedAgg(column='tpia_rec', aggfunc=lambda x: (x == 'Y').sum() / len(x))
).reset_index()

tpia_supp_peer_df = tpia_peer_rec_df[(tpia_peer_rec_df['tpia_calc_cnt'] < 50)]

# If the DataFrame is empty, there's no suppression at the "peer" level 

if tpia_peer_rec_df.empty:
    print("No suppression at peer level.")
    
if tpia_supp_peer_df.empty:
    print("No suppression at peer level.")




# For Region 
conditions = [
    pd.isna(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT']) | (ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'].str.strip() == ''),
    ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999',
    ~ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'].isin(['9999', ''])
]  

choices = ['B', 'N', 'Y']
ed_record_with_ucc_22['tpia_rec'] = np.select(conditions, choices, default=None)

# Filtering for tpia_reg_cnt
tpia_reg_cnt = ed_record_with_ucc_22[ed_record_with_ucc_22['tpia_rec'] != 'B']

#Aggregating data for tpia_reg_rec
aggregation = {
    'AM_CARE_KEY': 'count',
    'tpia_rec': [
        lambda x: (x == 'Y').sum(),
        lambda x: (x.isin(['Y', 'N'])).sum(),
        lambda x: (x == 'Y').mean()
    ]
}

tpia_reg_rec = tpia_reg_cnt.groupby(['SUBMISSION_FISCAL_YEAR', 'NEW_REGION_ID']).agg(aggregation)
tpia_reg_rec.columns = ['Total_CASE', 'tpia_calc_cnt', 'tpia_elig_cnt', 'tpia_rec_pct']
tpia_reg_rec.reset_index(inplace=True)

# Creating tpia_supp_reg and tpia_rpt_reg Dataframe using query method

condition_supp = "(tpia_calc_cnt < 50) | ((tpia_calc_cnt > 50) & (tpia_rec_pct < 0.75))"
condition_rpt = "(tpia_calc_cnt >= 50) | ((tpia_calc_cnt < 50) & (tpia_rec_pct >= 0.75))"

tpia_supp_reg = tpia_reg_rec.query(condition_supp)
tpia_rpt_reg = tpia_reg_rec.query(condition_rpt)



# Save suppression reports for province 

tpia_rpt_reg_v1_df = pd.merge(tpia_rpt_reg, df_fac[['NEW_REGION_ID', 'PROVINCE_NAME', 'REGION_E_DESC']],
                              left_on='NEW_REGION_ID', right_on='NEW_REGION_ID', how = 'left').drop_duplicates()

tpia_supp_reg_v1_df = pd.merge(tpia_supp_reg, df_fac[['NEW_REGION_ID', 'PROVINCE_NAME', 'REGION_E_DESC']],
                              left_on='NEW_REGION_ID', right_on='NEW_REGION_ID', how ='left').drop_duplicates()

tpia_rpt_org_v1_df = pd.merge(tpia_rpt_org, df_fac[['CORP_ID', 'PROVINCE_NAME', 'CORP_NAME']],
                              left_on='CORP_ID', right_on='CORP_ID', how= 'left').drop_duplicates()
tpia_supp_org_v1_df = pd.merge(tpia_supp_org, df_fac[['CORP_ID', 'PROVINCE_NAME', 'CORP_NAME']],
                               left_on='CORP_ID', right_on='CORP_ID', how = 'left').drop_duplicates()

