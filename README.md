# Update data for specific CORP_ID values 

corp_id_mapping = {
    1019: 81170, 
    10038: 81124, 
    7077: 80960, 
    5045: 81131, 
    5085: 81180, 
    5049: 81263, 
    5160: None 
}

# Apply the mapping to CORP_ID columns in all DataFrames
dataframes= [los_org_21, los_org_20, los_org_22_a, tpia_org_21, tpia_org_20, tpia_org_22_a]
for df in dataframes:
    df['CORP_ID'].replace(corp_id_mapping, inplace=True)

# Rename the 'PEER_GROUP_ID' column to 'CORP_PEER' in los_org_21 and los_org_20 & 
# Reaname the fiscal_year column to 'SUBMISSION_FISCAL_YEAR in los_reg_21 and tpia_reg_21
los_org_21.rename(columns={'PEER_GROUP_ID': 'CORP_PEER'}, inplace=True)
los_org_20.rename(columns={'PEER_GROUP_ID': 'CORP_PEER'}, inplace=True)
tpia_org_21.rename(columns={'PEER_GROUP_ID': 'CORP_PEER'}, inplace=True)
tpia_org_20.rename(columns={'PEER_GROUP_ID': 'CORP_PEER'}, inplace=True)

# los_reg_21.rename(columns={'SUBMISSION_FISCAL_YEAR': 'FISCAL_YEAR'}, inplace=True)
# tpia_reg_21.rename(columns={'SUBMISSION_FISCAL_YEAR': 'FISCAL_YEAR'}, inplace=True)
