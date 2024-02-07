# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Prepare dummy data for LOS
dummy_los = pd.DataFrame({column: [] for column in hsp_ind_organization_fact_los_final.columns})

# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].value_counts()[lambda x: x < 5].index:
    missing_years = [year for year in full_year_range if year not in hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()]
    dummy_los = pd.concat([
        dummy_los,
        pd.DataFrame({
            'ORGANIZATION_ID': [org_id] * len(missing_years),
            'FISCAL_YEAR_WH_ID': missing_years,
            'SEX_WH_ID': [3] * len(missing_years),
            'INDICATOR_CODE': ['033'] * len(missing_years),
            'INDICATOR_SUPPRESSION_CODE': ['999'] * len(missing_years),
            'IMPROVEMENT_IND_CODE': ['999'] * len(missing_years),
            'COMPARE_IND_CODE': ['999'] * len(missing_years),
            'INDICATOR_VALUE': [np.nan] * len(missing_years),
            'DATA_PERIOD_CODE': [f'0{year}' for year in missing_years],
            'DATA_PERIOD_TYPE_CODE': ['FY'] * len(missing_years),
            **{column: [0] * len(missing_years) if hsp_ind_organization_fact_los_final[column].dtype in ['int64', 'float64'] else ['999'] * len(missing_years) for column in hsp_ind_organization_fact_los_final.columns}
        })
    ])

# Append the dummy data to the original DataFrame
hsp_ind_organization_fact_los_33_d = pd.concat([
    hsp_ind_organization_fact_los_final,
    dummy_los,
    pd.DataFrame({'ORGANIZATION_ID': [7028], 'FISCAL_YEAR_WH_ID': [22], 'SEX_WH_ID': [3], 'INDICATOR_CODE': ['033'], 'INDICATOR_SUPPRESSION_CODE': ['006'], 'IMPROVEMENT_IND_CODE': ['999'], 'COMPARE_IND_CODE': ['999'], 'INDICATOR_VALUE': [np.nan], 'DATA_PERIOD_CODE': ['022'], 'DATA_PERIOD_TYPE_CODE': ['FY'], **{column: [np.nan] if hsp_ind_organization_fact_los_final[column].dtype in ['int64', 'float64'] else ['999'] for column in hsp_ind_organization_fact_los_final.columns}})
], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_los_33_d = hsp_ind_organization_fact_los_33_d.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_los_33_d = hsp_ind_organization_fact_los_33_d[hsp_ind_organization_fact_los_33_d['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]
