# Merge dataframes on specified columns and suffixes
merged_df = df_fac.merge(df_dq, 
                         left_on=['FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR'],
                         right_on=['FACILITY_AM_CARE_NUM', 'FISCAL_YEAR'],
                         suffixes=('_df_fac', '_df_dq'))

# Define a list of columns to keep
columns_to_keep = [
    'FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'CORP_ID',
    'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG'
]

# Add 'TYPE' column with values 'DQ' for merged_df
merged_df['TYPE'] = 'DQ'

# Create DataFrames t3 and t4 based on conditions
t3 = df_fac[df_fac['NACRS_ED_FLG'] == 1][columns_to_keep].copy()
t3['TYPE'] = 'SL'
t3['IND'] = ''

ps = df_ps[df_ps['FISCAL_YEAR'].astype(str) == '2022']
t4 = df_fac[df_fac['FACILITY_AM_CARE_NUM'].astype(str).isin(ps['FACILITY_AM_CARE_NUM'].astype(str))][columns_to_keep].copy()
t4['TYPE'] = 'PS'
t4['IND'] = ''

# Concatenate DataFrames t3, t4, and merged_df
result_df = pd.concat([t3, t4, merged_df], ignore_index=True)

# Remove duplicated columns and reset index
result_df = result_df[result_df.columns.drop_duplicates()].reset_index(drop=True)

# Filter df_fac based on CORP_ID
filtered_df_fac = df_fac[df_fac['CORP_ID'].isin(result_df['CORP_ID'])]

# Group by CORP_ID and count
tmp_cnt_ed_facility_org = filtered_df_fac.groupby('CORP_ID').size().reset_index(name='CORP_CNT')

# Merge result_df with tmp_cnt_ed_facility_org
ed_facility_org = result_df.merge(tmp_cnt_ed_facility_org, on='CORP_ID')

# Sort the DataFrame by 'TYPE' and 'FACILITY_AM_CARE_NUM'
ed_facility_org= ed_facility_org.sort_values(by=['TYPE', 'FACILITY_AM_CARE_NUM'])

# Rename columns 'REGION_NAME_x' and 'REGION_NAME_y' to 'REGION_NAME'
ed_facility_org = ed_facility_org.rename(columns={'REGION_NAME_x': 'REGION_NAME', 'REGION_NAME_y': 'REGION_NAME'})

# Remove duplicated columns and reset index
ed_facility_org = ed_facility_org[ed_facility_org.columns.drop_duplicates()].reset_index(drop=True)

# Specify the columns in the desired order
columns_to_keep = [
    'SUBMISSION_FISCAL_YEAR',	'FACILITY_AM_CARE_NUM','SITE_ID',	'CORP_ID',	'REGION_ID', 'PROVINCE_ID',	'TYPE',	'IND',	'NACRS_ED_FLG',	'CORP_CNT']

# Select the desired columns from the DataFrame
ed_facility_org = ed_facility_org[columns_to_keep]

# Display the resulting DataFrame
# display(ed_facility_org)
