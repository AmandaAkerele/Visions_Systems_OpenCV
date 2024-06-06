recreate this code in pyspark ensure accuracy in the calculations 

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
