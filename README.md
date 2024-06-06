accurately convert the code below to pyspark 

tpia_nt_22=calculate_percentile(TPIA_nt_record_ucc_df, 'WAIT_TIME_TO_PIA_HOURS', [0,0.5,0.9,0.999,1],confidence_interval=True)
tpia_nt_22=tpia_nt_22.rename(columns=lambda x: x.upper())

tpia_reg_22=calculate_percentile(TPIA_nt_record_ucc_df, 'WAIT_TIME_TO_PIA_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','FACILITY_PROVINCE','NEW_REGION_ID','REGION_E_DESC'])
tpia_reg_22=tpia_reg_22.rename(columns=lambda x: x.upper())

tpia_prov_22=calculate_percentile(TPIA_nt_record_ucc_df, 'WAIT_TIME_TO_PIA_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','FACILITY_PROVINCE','PROVINCE_ID','PROVINCE_NAME'])
tpia_prov_22=tpia_prov_22.rename(columns=lambda x: x.upper())

tpia_peer_22=calculate_percentile(ed_record_admit_22_Peer, 'WAIT_TIME_TO_PIA_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','SITE_PEER'])
tpia_peer_22=tpia_peer_22.rename(columns=lambda x: x.upper())

tpia_org_22=calculate_percentile(ed_record, 'WAIT_TIME_TO_PIA_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','CORP_ID','CORP_NAME','CORP_PEER'])
tpia_org_22=tpia_org_22.rename(columns=lambda x: x.upper())

tpia_site_22=calculate_percentile(ed_record, 'WAIT_TIME_TO_PIA_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','SITE_ID','SITE_NAME','SITE_PEER'])
tpia_site_22=tpia_site_22.rename(columns=lambda x: x.upper())

TPIA_site_Huron_Perth = tpia_site_22[tpia_site_22['SITE_ID'].isin([5096,5099,5103,5209])]
TPIA_site_Huron_Perth=TPIA_site_Huron_Perth.rename(columns=lambda x: x.upper())

# Filter tpia_site DataFrame based on the condition
filtered_rows = tpia_site_22[tpia_site_22['SITE_ID'].isin([5096, 5099, 5103, 5209])]

# Select only the required columns
filtered_rows = filtered_rows[['SUBMISSION_FISCAL_YEAR', 'SITE_ID','SITE_NAME', 'SITE_PEER', 'PERCENTILE_90']]

# Rename columns to match the tpia_org DataFrame structure
filtered_rows = filtered_rows.rename(columns={'SITE_ID': 'CORP_ID', 'SITE_NAME':'CORP_NAME', 'SITE_PEER':'CORP_PEER'})

tpia_org_22=pd.concat([tpia_org_22, filtered_rows], ignore_index=True)
