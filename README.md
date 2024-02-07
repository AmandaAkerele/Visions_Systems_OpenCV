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
