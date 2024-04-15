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
