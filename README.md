# Merge los_org_22_a with los_peer_base on 'CORP_PEER' column
los_org_cmp = los_org_22_a.merge(los_peer_base, on= 'CORP_PEER')

# Merge tpia_org_22_a with tpia_peer_base on 'CORP_PEER' column
tpia_org_cmp = tpia_org_22_a.merge(tpia_peer_base, on='CORP_PEER')

# Merge los_reg_22_a with los_reg_base on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
los_reg_cmp = los_reg_22_a.merge(los_reg_base, left_on='SUBMISSION_FISCAL_YEAR', right_on= 'SUBMISSION_FISCAL_YEAR')

# Merge tpia_reg_22 with tpia_reg_base on 'FISCAL_YEAR' and 'SUBMISSION_FISCAL_YEAR' columns
tpia_reg_cmp = tpia_reg_22_a.merge(tpia_reg_base, left_on='SUBMISSION_FISCAL_YEAR', right_on='SUBMISSION_FISCAL_YEAR')

los_org_cmp_a = los_org_cmp.rename(columns={0.2:'20th_Percentile',0.8:'80th_Percentile' })
tpia_org_cmp_a = tpia_org_cmp.rename(columns={0.2:'20th_Percentile',0.8:'80th_Percentile' })
los_reg_cmp_a = los_reg_cmp.rename(columns={0.2:'20th_Percentile',0.8:'80th_Percentile' })
tpia_reg_cmp_a = tpia_reg_cmp.rename(columns={0.2:'20th_Percentile',0.8:'80th_Percentile' })

def apply_conditions(df):
    conditions = [
        (df['PERCENTILE_90'] < df['20th_Percentile']),
        (df['PERCENTILE_90'] >= df['20th_Percentile']) & (df['PERCENTILE_90'] <= df['80th_Percentile']),
        (df['PERCENTILE_90'] > df['80th_Percentile'])
    ]

    values= ['001', '002', '003']
    descriptions = ['Above average performance', 'Same as average', 'Below average performance']

    # Apply conditions
    df['COMPARE_IND_CODE'] = np.select(conditions, values , default='')
    df['COMPARE_IND_E_DESC'] = np.select(conditions, descriptions, default='')

    # Sort the DataFrame by CORP_ID or REGION_ID as needed 
    if 'CORP_ID' in df.columns:
        df.sort_values(by=['CORP_ID'], inplace=True)
    elif 'REGION_ID' in df.columns:
        df.sort_values(by=['REGION_ID'], inplace=True)

# Apply conditions for each DataFrame
apply_conditions(los_org_cmp_a)
apply_conditions(tpia_org_cmp_a)
apply_conditions(los_reg_cmp_a)
apply_conditions(tpia_reg_cmp_a)

# Define a list of dataframes for los_org
los_org_dfs = [
    los_org_20[['CORP_ID', 'CORP_PEER']],
    los_org_21[['CORP_ID']],
    los_org_22_a[['CORP_ID']]
]

# Define a list of dataframes for tpia_org
tpia_org_dfs = [
    tpia_org_20[['CORP_ID', 'CORP_PEER']],
    tpia_org_21[['CORP_ID']],
    tpia_org_22_a[['CORP_ID']]
]


# Define a list of dataframes for los_reg
los_reg_dfs = [
    los_reg_20[['REGION_ID']],
    los_reg_21[['REGION_ID']],
    los_reg_22_a[['REGION_ID']]
]

# Define a list of dataframes for tpia_reg
tpia_reg_dfs = [
    tpia_reg_20[['REGION_ID']],
    tpia_reg_21[['REGION_ID']],
    tpia_reg_22_a[['REGION_ID']]
]

# Function to perform successive inner merges on a list of dataframes
def successive_inner_merge(dataframes):
    result_df = dataframes[0]
    for df in dataframes[1:]:
        result_df = pd.merge(result_df, df, on=result_df.columns.intersection(df.columns).tolist(), how='inner')
    return result_df

# Perform the successive merges for los_org
los_org_3x3 = successive_inner_merge(los_org_dfs)

# Perform the successive merges for tpia_org
tpia_org_3x3 = successive_inner_merge(tpia_org_dfs)

# Perform the successive merges for los_reg
los_reg_3x3 = successive_inner_merge(los_reg_dfs)

# Perform the successive merges for tpia_reg
tpia_reg_3x3 = successive_inner_merge(tpia_reg_dfs)
