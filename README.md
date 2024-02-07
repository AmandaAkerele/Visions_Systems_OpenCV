import pandas as pd
import numpy as np

# Assuming the DataFrame is already defined
hsp_ind_organization_fact_tpia_final = pd.DataFrame()  # Example DataFrame
hsp_organization_ext = pd.DataFrame()  # Example DataFrame for filtering

# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Create a DataFrame of all combinations of ORGANIZATION_ID and years
org_ids = hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].unique()
all_combinations = pd.DataFrame([(org_id, year) for org_id in org_ids for year in full_year_range],
                                columns=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])

# Merge with the existing data to find missing combinations
merged_data = pd.merge(all_combinations, hsp_ind_organization_fact_tpia_final, 
                       on=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'], 
                       how='left', indicator=True)

# Filter out existing data, leaving only missing combinations
missing_data = merged_data[merged_data['_merge'] == 'left_only'].copy()
missing_data.drop(columns=['_merge'], inplace=True)

# Assign default values to the missing data
default_values = {
    'SEX_WH_ID': 3, 'INDICATOR_CODE': '033', 'INDICATOR_SUPPRESSION_CODE': '999',
    'IMPROVEMENT_IND_CODE': '999', 'COMPARE_IND_CODE': '999', 'DATA_PERIOD_TYPE_CODE': 'FY',
    'INDICATOR_VALUE': np.nan
}
for column in hsp_ind_organization_fact_tpia_final.columns:
    if column not in ['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID']:
        missing_data[column] = default_values.get(column, np.nan)

# Append missing data to the original DataFrame
hsp_ind_organization_fact_tpia_34_a = pd.concat([hsp_ind_organization_fact_tpia_final, missing_data], ignore_index=True)

# Add blank rows for specific organizations (e.g., 7028)
orgs_blank_add = [7028]
for org_id in orgs_blank_add:
    for year in full_year_range:
        blank_row = {column: np.nan for column in hsp_ind_organization_fact_tpia_34_a.columns}
        blank_row['ORGANIZATION_ID'] = org_id
        blank_row['FISCAL_YEAR_WH_ID'] = year
        blank_row['SEX_WH_ID'] = 3  # Example: numeric column
        blank_row['INDICATOR_CODE'] = '033'
        blank_row['INDICATOR_SUPPRESSION_CODE'] = '006'
        # ... other column defaults as above
        hsp_ind_organization_fact_tpia_34_a = hsp_ind_organization_fact_tpia_34_a.append(blank_row, ignore_index=True)

# Sort and filter the DataFrame
hsp_ind_organization_fact_tpia_34_c = hsp_ind_organization_fact_tpia_34_a.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_tpia_34_d = hsp_ind_organization_fact_tpia_34_c[hsp_ind_organization_fact_tpia_34_c['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]
