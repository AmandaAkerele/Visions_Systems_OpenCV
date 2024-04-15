# Convert columns to numeric, just in case they are not
los_org_all_yr_b['PERCENTILE_90'] = pd.to_numeric(los_org_all_yr_b['PERCENTILE_90'], errors='coerce')
los_org_all_yr_b['TIME'] = pd.to_numeric(los_org_all_yr_b['TIME'], errors='coerce')

# Drop rows where either 'percentile_90' or 'time' is NaN after the conversion
los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'], inplace=True)

# Create a GroupBy object for the 'CORP_ID' variable
grouped = los_org_all_yr_b.groupby('CORP_ID')

# Create empty DataFrame to store results
los_org_trend = pd.DataFrame()

# Iterate over groups and perform regression
for group_name, group_data in grouped:
    X = sm.add_constant(group_data['TIME'])
    y = group_data['PERCENTILE_90']
    model = sm.OLS(y, X)
    results = model.fit()


   # Prepare list to store results
all_results = []

# Iterate over each group and perform regression
for group_name, group_data in grouped:
    X = sm.add_constant(group_data['TIME'])
    y = group_data['PERCENTILE_90']
    model = sm.OLS(y, X)
    results = model.fit()

    # Extract confidence intervals
    conf_int = results.conf_int(alpha=0.05)
    l95b = conf_int.loc['TIME', 0]  # Lower bound for 'time'
    u95b = conf_int.loc['TIME', 1]  # Upper bound for 'time'

    # Extracting results and appending to the list
    for param_type, values in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
                                  [results.params, results.bse, results.tvalues, results.pvalues,
                                   pd.Series([l95b], index=['TIME']), pd.Series([u95b], index=['TIME'])]):
        row = {'CORP_ID': group_name, '_TYPE_': param_type}
        row.update({name: values[name] for name in values.index if name in values})
        all_results.append(row)

# Convert results to DataFrame
los_org_trend = pd.DataFrame(all_results)

# Step 1: Creating los_org_trend_a with a new variable 'linr'
los_org_trend_a = los_org_trend.copy()
los_org_trend_a['linr'] = 0
los_org_trend_a.loc[(los_org_trend_a['_TYPE_'] == 'PVALUE') &
                    (los_org_trend_a['TIME'] < 0.05) &
                    (los_org_trend_a['TIME'].notna()), 'linr'] = 1

# Step 2: Creating subsets based on _TYPE_
los_org_p_val = los_org_trend_a[los_org_trend_a['_TYPE_'] == 'PVALUE'][['CORP_ID', 'linr']]
los_org_parms = los_org_trend_a[los_org_trend_a['_TYPE_'] == 'PARAMS'][['CORP_ID', 'TIME']]


# Steps 3 and 4: Sorting and merging the datasets
los_org_p_val_parms = pd.merge(los_org_p_val.sort_values('CORP_ID'),
                     los_org_parms.sort_values('CORP_ID'),
                     on='CORP_ID')

# Step 5: Assigning new variables based on conditions
los_org_p_val_parms['IMPROVEMENT_IND_CODE'] = '002'
los_org_p_val_parms['IMPROVEMENT_IND_E_DESC'] = 'No Change'

mask = los_org_p_val_parms['linr'] == 1
los_org_p_val_parms.loc[mask & (los_org_p_val_parms['TIME'] > 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['003', 'Weakening']
los_org_p_val_parms.loc[mask & (los_org_p_val_parms['TIME'] < 0), ['IMPROVEMENT_IND_CODE', 'IMPROVEMENT_IND_E_DESC']] = ['001', 'Improving']

# Step 6: Assuming ed_nacrs_flg_1 is another DataFrame you have
# Replace 'ed_nacrs_flg_1' with the actual DataFrame
los_org_trend_b = los_org_p_val_parms[~los_org_p_val_parms['CORP_ID'].isin(ed_nacrs_flg_1_SL['CORP_ID'])]
