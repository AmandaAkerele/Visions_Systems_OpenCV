# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Count the number of rows for each ORGANIZATION_ID
counts_los = hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].value_counts()

# Find ORGANIZATION_IDs with less than 5 rows
orgs_to_add_los = counts_los[counts_los < 5].index

# Define a function to get missing years
def missing_years_los(org_id):
    existing_years = hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return [year for year in full_year_range if year not in existing_years]

# Prepare dummy data
dummy_datalos = {column: [] for column in hsp_ind_organization_fact_los_final.columns}


# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in orgs_to_add_los:
    missing_years = missing_years_los(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_los_final.columns:
            if column == 'ORGANIZATION_ID':
                dummy_datalos[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                dummy_datalos[column].append(year)
            elif column == 'SEX_WH_ID':
                dummy_datalos[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                dummy_datalos[column].append('033')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                dummy_datalos[column].append('999')
            elif column=='IMPROVEMENT_IND_CODE':
                dummy_datalos[column].append('999')
            elif column =='COMPARE_IND_CODE':
                dummy_datalos[column].append('999')
            elif column =='INDICATOR_VALUE':
                dummy_datalos[column].append(np.nan)
            elif column =='DATA_PERIOD_CODE':
                dummy_datalos[column].append(f'0{year}')
            elif column == 'DATA_PERIOD_TYPE_CODE':
                dummy_datalos[column].append('FY')    
            else:   
                    if hsp_ind_organization_fact_los_final[column].dtype == 'int64' or hsp_ind_organization_fact_los_final[column].dtype == 'float64':
                        dummy_datalos[column].append(0)  # Default value for numeric columns
                    else:
                        dummy_datalos[column].append('999')  # Default value for string columns
                
# Convert dummy data to DataFrame
dummy_los = pd.DataFrame(dummy_datalos)

# Append the dummy data to the original DataFrame
hsp_ind_organization_fact_los_33_a = pd.concat([hsp_ind_organization_fact_los_final, dummy_los], ignore_index=True)

# Now create a list of orgs that are parial submissions, check if they already exist in the dataset, if not add blank rows for these org IDs. 7028 was added because of some reason

add_blank={'ORGANIZATION_ID' : [7028]}
add_blank_los=pd.DataFrame(add_blank)

blank_los = {column: [] for column in hsp_ind_organization_fact_los_final.columns}
# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in add_blank_los['ORGANIZATION_ID']:
    missing_years = missing_years_los(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_los_final.columns:
            if column == 'ORGANIZATION_ID':
                blank_los[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                blank_los[column].append(year)
            elif column == 'SEX_WH_ID':
                blank_los[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                blank_los[column].append('033')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                blank_los[column].append('006')
            elif column=='IMPROVEMENT_IND_CODE':
                blank_los[column].append('999')
            elif column =='COMPARE_IND_CODE':
                blank_los[column].append('999')
            elif column =='INDICATOR_VALUE':
                blank_los[column].append(np.nan)
            elif column =='DATA_PERIOD_CODE':
                blank_los[column].append(f'0{year}')
            elif column == 'DATA_PERIOD_TYPE_CODE':
                blank_los[column].append('FY')    
            else:   
                    if hsp_ind_organization_fact_los_final[column].dtype == 'int64' or hsp_ind_organization_fact_los_final[column].dtype == 'float64':
                        blank_los[column].append(np.nan)  # Default value for numeric columns
                    else:
                        blank_los[column].append('999')  # Default value for string columns

# Convert blank_los data to DataFrame
blank_los_add = pd.DataFrame(blank_los)

#Append the blank data to the original DataFrame
hsp_ind_organization_fact_los_33_b = pd.concat([hsp_ind_organization_fact_los_33_a, blank_los_add], ignore_index=True)


# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_los_33_c = hsp_ind_organization_fact_los_33_b.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_los_33_d = hsp_ind_organization_fact_los_33_c[hsp_ind_organization_fact_los_33_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]
# ///

#prepare datasets for applying suppression codes
los_supp_all_level = los_supp_org_22[['SUBMISSION_FISCAL_YEAR', 'CORP_ID']]
los_supp_all_level.rename(columns={'CORP_ID': 'ORGANIZATION_ID'}, inplace=True)
los_supp_all_level['SUPP_LEVEL'] = 'CORP'


region_count = los_supp_reg_22['REGION_ID'].value_counts().reset_index()
region_count.columns = ['REGION_ID', 'reg_cnt']
#Insert into los_supp_all_level from los_supp_reg where reg_cnt > 0
# Filter los_supp_reg first
filtered_los_supp_reg = los_supp_reg_22[los_supp_reg_22['REGION_ID'].isin(region_count[region_count['reg_cnt'] > 0]['REGION_ID'])]
# Prepare data for insertion
insert_los_reg = filtered_los_supp_reg[['REGION_ID']].copy()
insert_los_reg.rename(columns={'REGION_ID': 'ORGANIZATION_ID'}, inplace=True)
insert_los_reg['SUPP_LEVEL'] = 'REG'
# Append data
los_supp_all_level = pd.concat([los_supp_all_level,insert_los_reg], ignore_index=True)









# Create a list of organization_ids from los_supp_all_level
organization_ids_to_updateLOS = los_supp_all_level['ORGANIZATION_ID'].unique()

#now prepare datasets for DQ and PS
filtered_ID_DQ_LOS = ed_facility_org[
    (ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
    (ed_facility_org['TYPE'] == 'DQ') &
    (ed_facility_org['CORP_CNT'] == 1) &
    (ed_facility_org['IND'] == "ELOS")
]['CORP_ID'].unique()

filtered_ID_PS = ed_facility_org[
    (ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
    (ed_facility_org['TYPE'] == 'PS') &
    (ed_facility_org['CORP_CNT'] == 1)
]['CORP_ID'].unique()

def update_hsp_ind_organization_fact33(df):
    # Update 1
    #TPIA/ELOS Suppress Region and Province due to ED coverage (source from CAD OPS - DQ team);
    #Province  NS(2000), MB(600)
    #Region BC:Interior Health(9054) and Northern Health(9058)NS:Nova Scotia Health Authority Zone 1: Western(80286) and Nova Scotia Health Authority Zone 2: Northern(80287)SK: Far north zone (81404)			 
     #indicator_suppression_code = '901';
    condition = (
        (df['FISCAL_YEAR_WH_ID'] == 22) &
        (df['ORGANIZATION_ID'].isin([600, 2000,9054, 9058, 80286, 80287, 81404]))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    # Update 2
    condition = (
        (df['FISCAL_YEAR_WH_ID'] < 22) &
        (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))
    )
    df.loc[condition, 'IMPROVEMENT_IND_CODE'] = '999'

    # Update 3
    condition = (
        (df['FISCAL_YEAR_WH_ID'] < 22) &
        (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))
    )
    df.loc[condition, 'COMPARE_IND_CODE'] = '999'

    #Update 4
    condition = (
       (df['FISCAL_YEAR_WH_ID'] == 22) &
       (df['ORGANIZATION_ID'].isin(organization_ids_to_updateLOS))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['002', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    #Update 5 -if any DQ exists
    condition = (
       (df['FISCAL_YEAR_WH_ID'] == 22) &
       (df['ORGANIZATION_ID'].isin(filtered_ID_DQ_LOS))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['002', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    #Update 6 -if any partial submission apply code 006
    condition = (
       (df['FISCAL_YEAR_WH_ID'] == 22) &
       (df['ORGANIZATION_ID'].isin(filtered_ID_PS))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['006', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    return df

# Assuming hsp_ind_organization_fact33 is your DataFrame
hsp_ind_organization_fact33 = update_hsp_ind_organization_fact33(hsp_ind_organization_fact_los_33_d)


display(hsp_ind_organization_fact33)
