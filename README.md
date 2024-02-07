# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Count the number of rows for each ORGANIZATION_ID
counts_tpia = hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].value_counts()

# Find ORGANIZATION_IDs with less than 5 rows
orgs_to_add_tpia = counts_tpia[counts_tpia < 5].index

# Define a function to get missing years
def missing_years_tpia(org_id):
    existing_years = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return [year for year in full_year_range if year not in existing_years]

# Prepare dummy data
dummy_datatpia = {column: [] for column in hsp_ind_organization_fact_tpia_final.columns}


# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in orgs_to_add_tpia:
    missing_years = missing_years_tpia(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_tpia_final.columns:
            if column == 'ORGANIZATION_ID':
                dummy_datatpia[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                dummy_datatpia[column].append(year)
            elif column == 'SEX_WH_ID':
                dummy_datatpia[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                dummy_datatpia[column].append('033')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                dummy_datatpia[column].append('999')
            elif column=='IMPROVEMENT_IND_CODE':
                dummy_datatpia[column].append('999')
            elif column =='COMPARE_IND_CODE':
                dummy_datatpia[column].append('999')
            elif column =='INDICATOR_VALUE':
                dummy_datatpia[column].append(np.nan)
            elif column =='DATA_PERIOD_CODE':
                dummy_datatpia[column].append(f'0{year}')
            elif column == 'DATA_PERIOD_TYPE_CODE':
                dummy_datatpia[column].append('FY')    
            else:   
                    if hsp_ind_organization_fact_tpia_final[column].dtype == 'int64' or hsp_ind_organization_fact_tpia_final[column].dtype == 'float64':
                        dummy_datatpia[column].append(0)  # Default value for numeric columns
                    else:
                        dummy_datatpia[column].append('999')  # Default value for string columns
                
# Convert dummy data to DataFrame
dummy_tpia = pd.DataFrame(dummy_datatpia)

# Append the dummy data to the original DataFrame
hsp_ind_organization_fact_tpia_34_a = pd.concat([hsp_ind_organization_fact_tpia_final, dummy_tpia], ignore_index=True)

# Now create a list of orgs that are parial submissions, check if they already exist in the dataset, if not add blank rows for these org IDs. 7028 was added because of some reason

add_blank={'ORGANIZATION_ID' : [7028]}
add_blank_tpia=pd.DataFrame(add_blank)

blank_tpia = {column: [] for column in hsp_ind_organization_fact_tpia_final.columns}
# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in add_blank_tpia['ORGANIZATION_ID']:
    missing_years = missing_years_tpia(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_tpia_final.columns:
            if column == 'ORGANIZATION_ID':
                blank_tpia[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                blank_tpia[column].append(year)
            elif column == 'SEX_WH_ID':
                blank_tpia[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                blank_tpia[column].append('033')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                blank_tpia[column].append('006')
            elif column=='IMPROVEMENT_IND_CODE':
                blank_tpia[column].append('999')
            elif column =='COMPARE_IND_CODE':
                blank_tpia[column].append('999')
            elif column =='INDICATOR_VALUE':
                blank_tpia[column].append(np.nan)
            elif column =='DATA_PERIOD_CODE':
                blank_tpia[column].append(f'0{year}')
            elif column == 'DATA_PERIOD_TYPE_CODE':
                blank_tpia[column].append('FY')    
            else:   
                    if hsp_ind_organization_fact_tpia_final[column].dtype == 'int64' or hsp_ind_organization_fact_tpia_final[column].dtype == 'float64':
                        blank_tpia[column].append(np.nan)  # Default value for numeric columns
                    else:
                        blank_tpia[column].append('999')  # Default value for string columns

# Convert blank_tpia data to DataFrame
blank_tpia_add = pd.DataFrame(blank_tpia)

#Append the blank data to the original DataFrame
hsp_ind_organization_fact_tpia_34_b = pd.concat([hsp_ind_organization_fact_tpia_34_a, blank_tpia_add], ignore_index=True)


# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_tpia_34_c = hsp_ind_organization_fact_tpia_34_b.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_tpia_34_d = hsp_ind_organization_fact_tpia_34_c[hsp_ind_organization_fact_tpia_34_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]

