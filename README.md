import pandas as pd
import numpy as np

# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Function to get missing years for a given ORGANIZATION_ID
def missing_years_los(org_id, data_frame, year_range):
    existing_years = data_frame[data_frame['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return [year for year in year_range if year not in existing_years]

# Assuming hsp_ind_organization_fact_los_final is defined
# Example: hsp_ind_organization_fact_los_final = pd.DataFrame(...)

# Find ORGANIZATION_IDs with less than 5 rows and specific ORGANIZATION_IDs (e.g., 7028)
orgs_to_add_los = hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].value_counts()[lambda x: x < 5].index
specific_org_ids_los = [7028]

# Prepare and append dummy and blank data
dummy_and_blank_data_los = []

for org_id in orgs_to_add_los.union(specific_org_ids_los):
    for year in missing_years_los(org_id, hsp_ind_organization_fact_los_final, full_year_range):
        row = {'ORGANIZATION_ID': org_id, 'FISCAL_YEAR_WH_ID': year, 
               'SEX_WH_ID': 3, 'INDICATOR_CODE': '033', 'INDICATOR_SUPPRESSION_CODE': '999',
               'IMPROVEMENT_IND_CODE': '999', 'COMPARE_IND_CODE': '999', 
               'DATA_PERIOD_CODE': f'0{year}', 'DATA_PERIOD_TYPE_CODE': 'FY',
               'INDICATOR_VALUE': np.nan if org_id in specific_org_ids_los else 0}
        dummy_and_blank_data_los.append(row)

# Convert to DataFrame and append
dummy_and_blank_df_los = pd.DataFrame(dummy_and_blank_data_los)
hsp_ind_organization_fact_los_34_b = pd.concat([hsp_ind_organization_fact_los_final, dummy_and_blank_df_los], ignore_index=True)

# Sort the DataFrame
hsp_ind_organization_fact_los_34_c = hsp_ind_organization_fact_los_34_b.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])

# Optionally, filter the DataFrame
if 'hsp_organization_ext' in locals() or 'hsp_organization_ext' in globals():
    hsp_ind_organization_fact_los_34_d = hsp_ind_organization_fact_los_34_c[hsp_ind_organization_fact_los_34_c['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]
