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


4444
# Create standalone DataFrame
stand_alone = df_fac[df_fac['NACRS_ED_FLG'] == 1][['CORP_ID']]

#Create multiple_sites DataFrame
multiple_sites = df_fac[df_fac['CORP_ID'].isin(stand_alone['CORP_ID'])]
multiple_sites = multiple_sites.groupby('CORP_ID').size().reset_index(name='cnt_sites')
multiple_sites = multiple_sites[multiple_sites['cnt_sites'] > 1]         


# # Remove suppressed corp before calculating Baseline

# Get nacrs_ed_flg=1
ed_nacrs_flg_1_22 = df_fac[
    (df_fac['CORP_ID'].isin(
    ed_facility_org[
        (ed_facility_org['TYPE'] == 'SL') & 
        (ed_facility_org['CORP_CNT'] == 1)
        ]['CORP_ID']
    )) & 
    (df_fac['NACRS_ED_FLG'] ==1)
]



# Sort the resulting DataFrame by CORP_ID
ed_nacrs_flg_1_22.sort_values(by ='CORP_ID', inplace=True)

# Remove suppressed corp and non-reported corps
tpia_corp_conditions = ~tpia_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) & \
                       ~tpia_org_22['CORP_ID'].isin(tpia_supp_org['CORP_ID']) & \
                       ~tpia_org_22['CORP_ID'].isin(
                           ed_facility_org[(ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
                                           ((ed_facility_org['TYPE'] == 'PS') & (ed_facility_org['CORP_CNT'] == 1) |
                                            (ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['CORP_CNT'] == 1) & (ed_facility_org['IND'] == 'TPIA'))]['CORP_ID']
                       )
tpia_org_ta = tpia_org_22[tpia_corp_conditions]

# Remove suppressed corp and non-reported corps for ELOS 
los_corp_conditions = ~los_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) & \
                      ~los_org_22['CORP_ID'].isin(los_supp_org_22['CORP_ID']) & \
                      ~los_org_22['CORP_ID'].isin(
                          ed_facility_org[(ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
                                          ((ed_facility_org['TYPE'] == 'PS') & (ed_facility_org['CORP_CNT'] == 1) |
                                           (ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['CORP_CNT'] == 1) & (ed_facility_org['IND'] == 'ELOS'))]['CORP_ID']
                      )
los_org_ta = los_org_22[los_corp_conditions]

# Remove Huron Perth Healthcare Alliance, corp_id = 80228 from TPIA and ELOS
tpia_org_ta = tpia_org_ta[(tpia_org_ta['CORP_ID'] != 80228) & tpia_org_ta['CORP_PEER'].notna()]
los_org_ta = los_org_ta[(los_org_ta['CORP_ID'] != 80228) & los_org_ta['CORP_PEER'].notna()]

# Sort DataFrames
tpia_org_ta = tpia_org_ta.sort_values(by='CORP_ID')
los_org_ta = los_org_ta.sort_values(by='CORP_ID')



# Create tpia_reg_ta
tpia_reg_22_ta = tpia_reg_22[~tpia_reg_22['NEW_REGION_ID'].isin(tpia_supp_reg['NEW_REGION_ID'])]

# Create los_reg_ta
los_reg_22_ta= los_reg_22[~los_reg_22['NEW_REGION_ID'].isin(los_supp_reg_22['NEW_REGION_ID'])]


# Calculate percentile for TPIA
prct_20_80_tpia_org_22_ta = tpia_org_ta.groupby('CORP_PEER')['PERCENTILE_90'].quantile([0.2, 0.8]).unstack()

# Calculate percentile for ELOS
prct_20_80_los_org_22_ta= los_org_ta.groupby('CORP_PEER')['PERCENTILE_90'].quantile([0.2, 0.8]).unstack()


# *Regional Level Baseline - National: Since the regional values contain all UCC and stand-alone then the national one should include UCC and stand-alone;
# *Update on baselines: 20% tile and 80%tile of national regional value;

# Calculate percentile for national regional values for TPIA
prct_20_80_tpia_reg_22_ta= tpia_reg_22_ta.groupby('SUBMISSION_FISCAL_YEAR')['PERCENTILE_90'].quantile([0.2, 0.8]).unstack().reset_index()

# Calculate percentile for national regional values for ELOS
prct_20_80_los_reg_22_ta= los_reg_22_ta.groupby('SUBMISSION_FISCAL_YEAR')['PERCENTILE_90'].quantile([0.2, 0.8]).unstack().reset_index()

tpia_peer_base = prct_20_80_tpia_org_22_ta.copy()
los_peer_base = prct_20_80_los_org_22_ta.copy()
tpia_reg_base = prct_20_80_tpia_reg_22_ta.copy()
los_reg_base = prct_20_80_los_reg_22_ta.copy()


sasssssssssssssssssssssssss

sas.saslib('yr21', path=r"L:\Groups\CAD\YHS InDepth\YHS In-Depth 2022 Dec Release\DATASETS\MATCHED")
sas.saslib('yr20', path=r"L:\Groups\CAD\YHS InDepth\YHS In-Depth 2021 NovDec Release\Datasets\MATCHED")

# Los

#Create dataframe for 2021
los_reg_21 = sas.sasdata(table='los_reg_21', libref='yr21') 
los_reg_21=los_reg_21.to_df()
los_reg_21=los_reg_21.rename(columns=lambda x: x.upper())

los_prov_21 = sas.sasdata(table='los_prov_21', libref='yr21') 
los_prov_21=los_prov_21.to_df()
los_prov_21=los_prov_21.rename(columns=lambda x: x.upper())

los_org_21 = sas.sasdata(table='los_org_21', libref='yr21') 
los_org_21=los_org_21.to_df()
los_org_21=los_org_21.rename(columns=lambda x: x.upper())


#Create dataframe for 2020

los_reg_20 = sas.sasdata(table='los_reg_20', libref='yr20') 
los_reg_20=los_reg_20.to_df()
los_reg_20=los_reg_20.rename(columns=lambda x: x.upper())

los_prov_20 = sas.sasdata(table='los_prov_20', libref='yr20') 
los_prov_20=los_prov_20.to_df()
los_prov_20=los_prov_20.rename(columns=lambda x: x.upper())

los_org_20 = sas.sasdata(table='los_org_20', libref='yr20') 
los_org_20=los_org_20.to_df()
los_org_20=los_org_20.rename(columns=lambda x: x.upper())


sas.saslib('yr21', path=r"L:\Groups\CAD\YHS InDepth\YHS In-Depth 2022 Dec Release\DATASETS\MATCHED")
sas.saslib('yr20', path=r"L:\Groups\CAD\YHS InDepth\YHS In-Depth 2021 NovDec Release\Datasets\MATCHED")

# Tpia
#Create dataframe for 2021
tpia_org_21 = sas.sasdata(table='tpia_org_21', libref='yr21') 
tpia_org_21=tpia_org_21.to_df()
tpia_org_21=tpia_org_21.rename(columns=lambda x: x.upper())


tpia_prov_21 = sas.sasdata(table='tpia_prov_21', libref='yr21') 
tpia_prov_21=tpia_prov_21.to_df()
tpia_prov_21=tpia_prov_21.rename(columns=lambda x: x.upper())

tpia_reg_21 = sas.sasdata(table='tpia_reg_21', libref='yr21') 
tpia_reg_21=tpia_reg_21.to_df()
tpia_reg_21=tpia_reg_21.rename(columns=lambda x: x.upper())


#Create dataframe for 2020
tpia_org_20 = sas.sasdata(table='tpia_org_20', libref='yr20') 
tpia_org_20=tpia_org_20.to_df()
tpia_org_20=tpia_org_20.rename(columns=lambda x: x.upper())

tpia_prov_20 = sas.sasdata(table='tpia_prov_20', libref='yr20') 
tpia_prov_20=tpia_prov_20.to_df()
tpia_prov_20=tpia_prov_20.rename(columns=lambda x: x.upper())


tpia_reg_20 = sas.sasdata(table='tpia_reg_20', libref='yr20') 
tpia_reg_20=tpia_reg_20.to_df()
tpia_reg_20=tpia_reg_20.rename(columns=lambda x: x.upper())


# For Tpia_org_22
tpia_org_22_a = pd.merge(tpia_org_22, tpia_supp_org[['CORP_ID']], on='CORP_ID', how='left', indicator=True)
tpia_org_22_a = tpia_org_22_a[tpia_org_22_a['_merge'] == 'left_only'].drop(columns=['_merge'])
tpia_org_22_a = tpia_org_22_a.rename(columns={'SUBMISSION_FISCAL_YEAR': 'FISCAL_YEAR'})
tpia_org_22_a['CORP_ID'].replace({5085: 81180, 5049:81263}, inplace=True)


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

# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    pd.concat([los_org_20, los_org_21, los_org_22_a], ignore_index=True)
      .merge(los_org_3x3[['CORP_ID']], on='CORP_ID', how='inner')
      .rename(columns={'FISCAL_YEAR': 'TIME'})
)

# Concatenate, merge, and rename for Tpia Org
tpia_org_all_yr_b = (
    pd.concat([tpia_org_20, tpia_org_21, tpia_org_22_a], ignore_index= True)
      .merge(tpia_org_3x3[['CORP_ID']], on='CORP_ID', how='inner')
      .rename(columns={'FISCAL_YEAR': 'TIME'})
)


# Concatenate, merge and rename for los_reg
los_reg_all_yr_b = (
    pd.concat([los_reg_20, los_reg_21, los_reg_22_a], ignore_index=True)
      .merge(los_reg_3x3[['REGION_ID']], on='REGION_ID', how='inner')
      .rename(columns={'SUBMISSION_FISCAL_YEAR': 'TIME'})
)

# Concatenate, merge and rename for TPIA Reg
tpia_reg_all_yr_b = (
    pd.concat([tpia_reg_20, tpia_reg_21, tpia_reg_22_a], ignore_index=True)
      .merge(tpia_reg_3x3[['REGION_ID']], on='REGION_ID', how='inner')
      .rename(columns={'SUBMISSION_FISCAL_YEAR': 'TIME'})
)


# Convert columns to numeric and drop NaNs
los_org_all_yr_b = los_org_all_yr_b.apply(pd.to_numeric, errors='coerce')
los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'], inplace=True)

# Create a GroupBy object and prepare DataFrame for results
grouped = los_org_all_yr_b.groupby('CORP_ID')
all_results = []

# Iterate, perform regression, and store results
for group_name, group_data in grouped:
    X = sm.add_constant(group_data['TIME'])
    y = group_data['PERCENTILE_90']
    model = sm.OLS(y, X).fit()
    conf_int = model.conf_int(alpha=0.05)

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [model.params, model.bse, model.tvalues, model.pvalues,
                                   pd.Series([conf_int.loc['TIME', 0]], index=['TIME']),
                                   pd.Series([conf_int.loc['TIME', 1]], index=['TIME'])]):
        all_results.append({'CORP_ID': group_name, '_TYPE_': param_type, **values})

# Convert to DataFrame and analyze results
los_org_trend_a = pd.DataFrame(all_results)
los_org_trend_a['linr'] = ((los_org_trend_a['_TYPE_'] == 'PVALUE') & (los_org_trend_a['TIME'] < 0.05)).astype(int)

# Create subsets and merge
p_val = los_org_trend_a[los_org_trend_a['_TYPE_'] == 'PVALUE'][['CORP_ID', 'linr']]
parms = los_org_trend_a[los_org_trend_a['_TYPE_'] == 'PARAMS'][['CORP_ID', 'TIME']]
merged = pd.merge(p_val, parms, on='CORP_ID')

# Define improvement indicators
merged['IMPROVEMENT_IND_CODE'] = '002'
merged['IMPROVEMENT_IND_E_DESC'] = 'No Change'
mask = merged['linr'] == 1
merged.loc[mask & (merged['TIME'] > 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['003', 'Weakening']
merged.loc[mask & (merged['TIME'] < 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['001', 'Improving']

# Final filtering 
los_org_trend_b = merged[~merged['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID'])]


# Convert columns to numeric and drop NaNs
tpia_org_all_yr_b = tpia_org_all_yr_b.apply(pd.to_numeric, errors='coerce')
tpia_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'], inplace=True)

# Create a GroupBy object and prepare DataFrame for results
grouped = tpia_org_all_yr_b.groupby('CORP_ID')
all_results = []

# Iterate, perform regression, and store results
for group_name, group_data in grouped:
    X = sm.add_constant(group_data['TIME'])
    y = group_data['PERCENTILE_90']
    model = sm.OLS(y, X).fit()
    conf_int = model.conf_int(alpha=0.05)

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [model.params, model.bse, model.tvalues, model.pvalues,
                                   pd.Series([conf_int.loc['TIME', 0]], index=['TIME']),
                                   pd.Series([conf_int.loc['TIME', 1]], index=['TIME'])]):
        all_results.append({'CORP_ID': group_name, '_TYPE_': param_type, **values})

# Convert to DataFrame and analyze results
tpia_org_trend_a = pd.DataFrame(all_results)
tpia_org_trend_a['linr'] = ((tpia_org_trend_a['_TYPE_'] == 'PVALUE') & (tpia_org_trend_a['TIME'] < 0.05)).astype(int)

# Create subsets and merge
p_val = tpia_org_trend_a[tpia_org_trend_a['_TYPE_'] == 'PVALUE'][['CORP_ID', 'linr']]
parms = tpia_org_trend_a[tpia_org_trend_a['_TYPE_'] == 'PARAMS'][['CORP_ID', 'TIME']]
merged = pd.merge(p_val, parms, on='CORP_ID')

# Define improvement indicators
merged['IMPROVEMENT_IND_CODE'] = '002'
merged['IMPROVEMENT_IND_E_DESC'] = 'No Change'
mask = merged['linr'] == 1
merged.loc[mask & (merged['TIME'] > 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['003', 'Weakening']
merged.loc[mask & (merged['TIME'] < 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['001', 'Improving']

# Final filtering 
tpia_org_trend_b = merged[~merged['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID'])]
# display(tpia_org_all_yr_b)
# display(tpia_org_trend_a)
# display(p_val)
# display(parms)
# display(tpia_org_trend_b)


# Convert columns to numeric and drop NaNs
los_reg_all_yr_b = los_reg_all_yr_b.apply(pd.to_numeric, errors='coerce')
los_reg_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'], inplace=True)

# Create a GroupBy object and prepare DataFrame for results
grouped = los_reg_all_yr_b.groupby('REGION_ID')
all_results = []

# Iterate, perform regression, and store results
for group_name, group_data in grouped:
    X = sm.add_constant(group_data['TIME'])
    y = group_data['PERCENTILE_90']
    model = sm.OLS(y, X).fit()
    conf_int = model.conf_int(alpha=0.05)

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [model.params, model.bse, model.tvalues, model.pvalues,
                                   pd.Series([conf_int.loc['TIME', 0]], index=['TIME']),
                                   pd.Series([conf_int.loc['TIME', 1]], index=['TIME'])]):
        all_results.append({'REGION_ID': group_name, '_TYPE_': param_type, **values})

# Convert to DataFrame and analyze results
los_reg_trend_a = pd.DataFrame(all_results)
los_reg_trend_a['linr'] = ((los_reg_trend_a['_TYPE_'] == 'PVALUE') & (los_reg_trend_a['TIME'] < 0.05)).astype(int)

# Create subsets and merge
p_val = los_reg_trend_a[los_reg_trend_a['_TYPE_'] == 'PVALUE'][['REGION_ID', 'linr']]
parms = los_reg_trend_a[los_reg_trend_a['_TYPE_'] == 'PARAMS'][['REGION_ID', 'TIME']]
merged = pd.merge(p_val, parms, on='REGION_ID')

# Define improvement indicators
merged['IMPROVEMENT_IND_CODE'] = '002'
merged['IMPROVEMENT_IND_E_DESC'] = 'No Change'
mask = merged['linr'] == 1
merged.loc[mask & (merged['TIME'] > 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['003', 'Weakening']
merged.loc[mask & (merged['TIME'] < 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['001', 'Improving']

# Final filtering 
los_reg_trend_b = merged[~merged['REGION_ID'].isin(ed_nacrs_flg_1_22['REGION_ID'])]

# display(los_reg_trend_a)
# display(p_val)
# display(parms)
# display(los_reg_trend_b)


# Convert columns to numeric and drop NaNs
tpia_reg_all_yr_b = tpia_reg_all_yr_b.apply(pd.to_numeric, errors='coerce')
tpia_reg_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'], inplace=True)

# Create a GroupBy object and prepare DataFrame for results
grouped = tpia_reg_all_yr_b.groupby('REGION_ID')
all_results = []

# Iterate, perform regression, and store results
for group_name, group_data in grouped:
    X = sm.add_constant(group_data['TIME'])
    y = group_data['PERCENTILE_90']
    model = sm.OLS(y, X).fit()
    conf_int = model.conf_int(alpha=0.05)

    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [model.params, model.bse, model.tvalues, model.pvalues,
                                   pd.Series([conf_int.loc['TIME', 0]], index=['TIME']),
                                   pd.Series([conf_int.loc['TIME', 1]], index=['TIME'])]):
        all_results.append({'REGION_ID': group_name, '_TYPE_': param_type, **values})

# Convert to DataFrame and analyze results
tpia_reg_trend_a = pd.DataFrame(all_results)
tpia_reg_trend_a['linr'] = ((tpia_reg_trend_a['_TYPE_'] == 'PVALUE') & (tpia_reg_trend_a['TIME'] < 0.05)).astype(int)

# Create subsets and merge
p_val = tpia_reg_trend_a[tpia_reg_trend_a['_TYPE_'] == 'PVALUE'][['REGION_ID', 'linr']]
parms = tpia_reg_trend_a[tpia_reg_trend_a['_TYPE_'] == 'PARAMS'][['REGION_ID', 'TIME']]
merged = pd.merge(p_val, parms, on='REGION_ID')

# Define improvement indicators
merged['IMPROVEMENT_IND_CODE'] = '002'
merged['IMPROVEMENT_IND_E_DESC'] = 'No Change'
mask = merged['linr'] == 1
merged.loc[mask & (merged['TIME'] > 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['003', 'Weakening']
merged.loc[mask & (merged['TIME'] < 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['001', 'Improving']

# Final filtering 
tpia_reg_trend_b = merged[~merged['REGION_ID'].isin(ed_nacrs_flg_1_22['REGION_ID'])]

# display(tpia_reg_trend_a)
# display(p_val)
# display(parms)
# display(tpia_reg_trend_b)
