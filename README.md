correctly convert this code from python to pyspark 

# For Tpia_org_22
tpia_org_22_a = pd.merge(tpia_org_22, tpia_supp_org[['CORP_ID']], on='CORP_ID', how='left', indicator=True)
tpia_org_22_a = tpia_org_22_a[tpia_org_22_a['_merge'] == 'left_only'].drop(columns=['_merge'])
tpia_org_22_a = tpia_org_22_a.rename(columns={'SUBMISSION_FISCAL_YEAR': 'FISCAL_YEAR'})
tpia_org_22_a['CORP_ID'].replace({5085: 81180, 5049:81263}, inplace=True)

# Tpia_reg_22
tpia_reg_22_a = pd.merge(tpia_reg_22, tpia_supp_reg[['NEW_REGION_ID']], on='NEW_REGION_ID', how='left', indicator=True)
tpia_reg_22_a = tpia_reg_22_a[tpia_reg_22_a['_merge'] == 'left_only'].drop(columns=['_merge'])
tpia_reg_22_a = tpia_reg_22_a.rename(columns={'NEW_REGION_ID': 'REGION_ID'})

# For Los_org_22
los_org_22_a = pd.merge(los_org_22, los_supp_org_22[['CORP_ID']], on='CORP_ID', how='left', indicator=True)
los_org_22_a = los_org_22_a[los_org_22_a['_merge'] == 'left_only'].drop(columns=['_merge'])
los_org_22_a = los_org_22_a.rename(columns={'SUBMISSION_FISCAL_YEAR': 'FISCAL_YEAR'})
los_org_22_a['CORP_ID'].replace({5085: 81180, 5049:81263}, inplace=True)

# # For Los_reg_22
los_reg_22_a = pd.merge(los_reg_22, los_supp_reg_22[['NEW_REGION_ID']], on='NEW_REGION_ID', how='left', indicator=True)
los_reg_22_a = los_reg_22_a[los_reg_22_a['_merge'] == 'left_only'].drop(columns=['_merge'])
los_reg_22_a = los_reg_22_a.rename(columns={'NEW_REGION_ID': 'REGION_ID'})

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

# Filter out rows where CORP_ID is 5160
los_org_21 = los_org_21[los_org_21['CORP_ID'] != 5160]
los_org_20 = los_org_20[los_org_20['CORP_ID'] != 5160]

# Merge los_org_22_a with los_peer_base on 'CORP_PEER' column
los_org_cmp = los_org_22_a.merge(los_peer_base, on= 'CORP_PEER')

# Merge tpia_org_22_a with tpia_peer_base on 'CORP_PEER' column
tpia_org_cmp = tpia_org_22_a.merge(tpia_peer_base, on='CORP_PEER')

# Merge los_reg_22_a with los_reg_base on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
los_reg_cmp = los_reg_22_a.merge(los_reg_base, left_on='SUBMISSION_FISCAL_YEAR', right_on= 'SUBMISSION_FISCAL_YEAR')

# Merge tpia_reg_22 with tpia_reg_base on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
tpia_reg_cmp = tpia_reg_22_a.merge(tpia_reg_base, left_on='SUBMISSION_FISCAL_YEAR', right_on='SUBMISSION_FISCAL_YEAR')
