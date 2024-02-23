los_nt_22=calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS', [0,0.5,0.9,0.999,1],confidence_interval=True)
los_nt_22=los_nt_22.rename(columns=lambda x: x.upper())

los_reg_22=calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','FACILITY_PROVINCE','NEW_REGION_ID','REGION_E_DESC'])
los_reg_22=los_reg_22.rename(columns=lambda x: x.upper())

los_prov_22=calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','FACILITY_PROVINCE','PROVINCE_ID','PROVINCE_NAME'])
los_prov_22=los_prov_22.rename(columns=lambda x: x.upper())

los_peer_22=calculate_percentile(ed_record_admit_22_Peer, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','SITE_PEER'])
los_peer_22=los_peer_22.rename(columns=lambda x: x.upper())

los_org_22=calculate_percentile(ed_record_admit_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','CORP_ID','CORP_NAME','CORP_PEER'])
los_org_22=los_org_22.rename(columns=lambda x: x.upper())

los_site_22=calculate_percentile(ed_record_admit_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','SITE_ID','SITE_NAME','SITE_PEER'])
los_site_22=los_site_22.rename(columns=lambda x: x.upper())

LOS_site_Huron_Perth = los_site_22[los_site_22['SITE_ID'].isin([5096,5099,5103,5209])]
LOS_site_Huron_Perth=LOS_site_Huron_Perth.rename(columns=lambda x: x.upper())

# Filter tpia_site DataFrame based on the condition
filtered_rows = los_site_22[los_site_22['SITE_ID'].isin([5096, 5099, 5103, 5209])]

# Select only the required columns
filtered_rows = filtered_rows[['SUBMISSION_FISCAL_YEAR', 'SITE_ID','SITE_NAME', 'SITE_PEER', 'PERCENTILE_90']]

# Rename columns to match the tpia_org DataFrame structure
filtered_rows = filtered_rows.rename(columns={'SITE_ID': 'CORP_ID', 'SITE_NAME':'CORP_NAME', 'SITE_PEER':'CORP_PEER'})

los_org_22=pd.concat([los_org_22, filtered_rows], ignore_index=True)
