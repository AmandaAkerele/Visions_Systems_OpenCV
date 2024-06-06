correctly convert this code below to pyspark 

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
