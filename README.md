
# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Create a DataFrame of all combinations of ORGANIZATION_ID and years
org_ids_los = hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].unique()
all_combinations_los = pd.DataFrame([(org_id, year) for org_id in org_ids_los for year in full_year_range],
                                    columns=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])

# Merge with the existing data to find missing combinations
merged_data_los = pd.merge(all_combinations_los, hsp_ind_organization_fact_los_final, 
                           on=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'], 
                           how='left', indicator=True)

# Filter out existing data, leaving only missing combinations
missing_data_los = merged_data_los[merged_data_los['_merge'] == 'left_only'].copy()
missing_data_los.drop(columns=['_merge'], inplace=True)

# Assign default values to the missing data
default_values_los = {
    # ... (same as before)
}

for column in hsp_ind_organization_fact_los_final.columns:
    if column not in ['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID']:
        missing_data_los[column] = default_values_los.get(column, np.nan)

# Append missing data to the original DataFrame
hsp_ind_organization_fact_los_33_a = pd.concat([hsp_ind_organization_fact_los_final, missing_data_los], ignore_index=True)

# Add blank rows for specific organizations (e.g., 7028)
orgs_blank_add_los = [7028]
blank_rows_los = []

for org_id in orgs_blank_add_los:
    for year in full_year_range:
        blank_row_los = {column: np.nan for column in hsp_ind_organization_fact_los_33_a.columns}
        blank_row_los['ORGANIZATION_ID'] = org_id
        blank_row_los['FISCAL_YEAR_WH_ID'] = year
        # ... other column defaults as before
        blank_rows_los.append(blank_row_los)

# Convert list of dicts to DataFrame and append
blank_rows_df_los = pd.DataFrame(blank_rows_los)
hsp_ind_organization_fact_los_33_a = pd.concat([hsp_ind_organization_fact_los_33_a, blank_rows_df_los], ignore_index=True)

# Sort and filter the DataFrame
hsp_ind_organization_fact_los_33_c = hsp_ind_organization_fact_los_33_a.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_los_33_d = hsp_ind_organization_fact_los_33_c[hsp_ind_organization_fact_los_33_c['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]
