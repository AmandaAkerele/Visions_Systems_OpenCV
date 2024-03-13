# Create file for shallow slice piolt 
# Contextual Measure: Patient Days in Alternate Level of Care (Percentage)
ALC_LOS_CORP_1 = ALC_LOS_CORP[["ORGANIZATION_ID", "PCT_ALC_LOS"]]
ALC_LOS_CORP_1.rename(columns={"ORGANIZATION_ID":"reporting_entity_code", "PCT_ALC_LOS": "metric_result"}, inplace=True)

ALC_REG_1 = ALC_REG[["REGIONAL_ORGANIZATION_ID", "PCT_ALC_LOS"]]
ALC_REG_1.rename(columns={"REGIONAL_ORGANIZATION_ID":"reporting_entity_code", "PCT_ALC_LOS": "metric_result"}, inplace=True)

ALC_PROV_1 = ALC_PROV[["PROVINCIAL_ORGANIZATION_ID", "PCT_ALC_LOS"]]
ALC_PROV_1.rename(columns={"PROVINCIAL_ORGANIZATION_ID":"reporting_entity_code", "PCT_ALC_LOS": "metric_result"}, inplace=True)

ALC_File = pd.concat([ALC_LOS_CORP_1, ALC_REG_1, ALC_PROV_1], ignore_index=True)

ALC_File['reporting_period_code'] = 'FY20' + yr
ALC_File['reporting_entity_type_code'] = 'ORG'
ALC_File['indicator_code'] = '816'
ALC_File['metric_code'] = 'PERCENT'
ALC_File['breakdown_type_code_l1'] = 'N/A'
ALC_File['breakdown_value_code_l1'] = 'N/A'
ALC_File['breakdown_type_code_l2'] = 'N/A'
ALC_File['breakdown_value_code_l2'] = 'N/A'
ALC_File['metric_descriptor_group_code'] = ''
ALC_File['metric_descriptor_code'] = ''
ALC_File['missing_reason_code'] = ''
ALC_File['public_metric_result'] = ALC_File['metric_result']

ALC_File = ALC_File[['reporting_period_code',	'reporting_entity_code','reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]
# ALC_File.to_csv('816_agg_20' + yr + '.csv', index=False)



# Create file for shallow slice piolt 
# Contextual Measure: Number of Emergency Department Visits
ED_CORP_File =ED_CORP[["CORP_ID", "ED_CORP_CNT"]]
ED_CORP_File.rename(columns={"CORP_ID":"reporting_entity_code", "ED_CORP_CNT": "metric_result"}, inplace=True)

ED_CORP_File['reporting_period_code'] = 'FY20' + yr
ED_CORP_File['reporting_entity_type_code'] = 'ORG'
ED_CORP_File['indicator_code'] = '814'
ED_CORP_File['metric_code'] = 'NUMBER_OF_CASES'
ED_CORP_File['breakdown_type_code_l1'] = 'N/A'
ED_CORP_File['breakdown_value_code_l1'] = 'N/A'
ED_CORP_File['breakdown_type_code_l2'] = 'N/A'
ED_CORP_File['breakdown_value_code_l2'] = 'N/A'
ED_CORP_File['metric_descriptor_group_code'] = ''
ED_CORP_File['metric_descriptor_code'] = ''
ED_CORP_File['missing_reason_code'] = ''
ED_CORP_File['public_metric_result'] = ED_CORP_File['metric_result']

ED_CORP_File = ED_CORP_File[['reporting_period_code',	'reporting_entity_code','reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]
# ED_CORP_File.to_csv('814_agg_20' + yr + '.csv', index=False)

# Create file for shallow slice pilot 
# Contextual Measure: Patients Admitted Through the Emergency Department
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All[["ORGANIZATION_ID",  "PCT_ADMT_ED"]]
IP_admit_ED_CORP_All_File.rename(columns={"ORGANIZATION_ID":"reporting_entity_code", "PCT_ADMT_ED": "metric_result"}, inplace=True)

IP_admit_ED_CORP_All_File['reporting_period_code'] = 'FY20' + yr
IP_admit_ED_CORP_All_File['reporting_entity_type_code'] = 'ORG'
IP_admit_ED_CORP_All_File['indicator_code'] = '815'
IP_admit_ED_CORP_All_File['metric_code'] = 'PERCENT'
IP_admit_ED_CORP_All_File['breakdown_type_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_type_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['metric_descriptor_group_code'] = ''
IP_admit_ED_CORP_All_File['metric_descriptor_code'] = ''

# Fill NaN values with "M02" in missing_reason_code column
IP_admit_ED_CORP_All_File['missing_reason_code'] = IP_admit_ED_CORP_All_File['metric_result'].isna().astype(int).map({1: 'M02', 0: ''})
IP_admit_ED_CORP_All_File['public_metric_result'] = IP_admit_ED_CORP_All_File['metric_result']

# Reorder columns
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
IP_admit_ED_CORP_All_File.to_csv('815_agg_20' + yr + '.csv', index=False)



# Create file for shallow slice pilot 
# Contextual Measure: Total Acute Care Resource Use Intensity
IP_RIW_CORP_TOTAL_File =IP_RIW_CORP_All[["ORGANIZATION_ID",  "RIW_SUM"]]
IP_RIW_CORP_TOTAL_File.rename(columns={"ORGANIZATION_ID":"reporting_entity_code", "RIW_SUM": "metric_result"}, inplace=True)

IP_RIW_CORP_TOTAL_File['reporting_period_code'] = 'FY20' + yr
IP_RIW_CORP_TOTAL_File['reporting_entity_type_code'] = 'ORG'
IP_RIW_CORP_TOTAL_File['indicator_code'] = '817'
IP_RIW_CORP_TOTAL_File['metric_code'] = 'TOTAL'
IP_RIW_CORP_TOTAL_File['breakdown_type_code_l1'] = 'N/A'
IP_RIW_CORP_TOTAL_File['breakdown_value_code_l1'] = 'N/A'
IP_RIW_CORP_TOTAL_File['breakdown_type_code_l2'] = 'N/A'
IP_RIW_CORP_TOTAL_File['breakdown_value_code_l2'] = 'N/A'
IP_RIW_CORP_TOTAL_File['metric_descriptor_group_code'] = ''
IP_RIW_CORP_TOTAL_File['metric_descriptor_code'] = ''
IP_RIW_CORP_TOTAL_File['missing_reason_code'] = ''
IP_RIW_CORP_TOTAL_File['public_metric_result'] = IP_RIW_CORP_TOTAL_File['metric_result']

IP_RIW_CORP_TOTAL_File = IP_RIW_CORP_TOTAL_File[['reporting_period_code',	'reporting_entity_code','reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]
IP_RIW_CORP_TOTAL_File.to_csv('817_agg_20' + yr + '.csv', index=False)


# Create file for shallow slice pilot 
# Contextual Measure: Average Acute Care Resource Use Intensity
IP_RIW_CORP_AVG_File =IP_RIW_CORP_All[["ORGANIZATION_ID",  "RIW_AVG"]]
IP_RIW_CORP_AVG_File.rename(columns={"ORGANIZATION_ID":"reporting_entity_code", "RIW_AVG": "metric_result"}, inplace=True)

IP_RIW_CORP_AVG_File['reporting_period_code'] = 'FY20' + yr
IP_RIW_CORP_AVG_File['reporting_entity_type_code'] = 'ORG'
IP_RIW_CORP_AVG_File['indicator_code'] = '818'
IP_RIW_CORP_AVG_File['metric_code'] = 'AVERAGE'
IP_RIW_CORP_AVG_File['breakdown_type_code_l1'] = 'N/A'
IP_RIW_CORP_AVG_File['breakdown_value_code_l1'] = 'N/A'
IP_RIW_CORP_AVG_File['breakdown_type_code_l2'] = 'N/A'
IP_RIW_CORP_AVG_File['breakdown_value_code_l2'] = 'N/A'
IP_RIW_CORP_AVG_File['metric_descriptor_group_code'] = ''
IP_RIW_CORP_AVG_File['metric_descriptor_code'] = ''
IP_RIW_CORP_AVG_File['missing_reason_code'] = ''
IP_RIW_CORP_AVG_File['public_metric_result'] = IP_RIW_CORP_AVG_File['metric_result']

IP_RIW_CORP_AVG_File = IP_RIW_CORP_AVG_File[['reporting_period_code',	'reporting_entity_code','reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]
IP_RIW_CORP_AVG_File.to_csv('818_agg_20' + yr + '.csv', index=False)


# Create file for shallow slice pilot 
# Contextual Measure: Number of Acute Care Hospital Stays
IP_LOS_CORP_TOTAL_File =ip_los_corp_All[["ORGANIZATION_ID",  "SUM_TOTAL_LOS"]]
IP_LOS_CORP_TOTAL_File.rename(columns={"ORGANIZATION_ID":"reporting_entity_code", "SUM_TOTAL_LOS": "metric_result"}, inplace=True)

IP_LOS_CORP_TOTAL_File['reporting_period_code'] = 'FY20' + yr
IP_LOS_CORP_TOTAL_File['reporting_entity_type_code'] = 'ORG'
IP_LOS_CORP_TOTAL_File['indicator_code'] = '812'
IP_LOS_CORP_TOTAL_File['metric_code'] = 'TOTAL'
IP_LOS_CORP_TOTAL_File['breakdown_type_code_l1'] = 'N/A'
IP_LOS_CORP_TOTAL_File['breakdown_value_code_l1'] = 'N/A'
IP_LOS_CORP_TOTAL_File['breakdown_type_code_l2'] = 'N/A'
IP_LOS_CORP_TOTAL_File['breakdown_value_code_l2'] = 'N/A'
IP_LOS_CORP_TOTAL_File['metric_descriptor_group_code'] = ''
IP_LOS_CORP_TOTAL_File['metric_descriptor_code'] = ''
IP_LOS_CORP_TOTAL_File['missing_reason_code'] = ''
IP_LOS_CORP_TOTAL_File['public_metric_result'] = IP_LOS_CORP_TOTAL_File['metric_result']

IP_LOS_CORP_TOTAL_File = IP_LOS_CORP_TOTAL_File[['reporting_period_code',	'reporting_entity_code','reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]
IP_LOS_CORP_TOTAL_File.to_csv('812_agg_20' + yr + '.csv', index=False)



# Create file for shallow slice pilot 
# Contextual Measure: Average Length of a Hospital Stay (Days)
IP_LOS_CORP_AVG_File =ip_los_corp_All[["ORGANIZATION_ID",  "AVG_LOS"]]
IP_LOS_CORP_AVG_File.rename(columns={"ORGANIZATION_ID":"reporting_entity_code", "AVG_LOS": "metric_result"}, inplace=True)

IP_LOS_CORP_AVG_File['reporting_period_code'] = 'FY20' + yr
IP_LOS_CORP_AVG_File['reporting_entity_type_code'] = 'ORG'
IP_LOS_CORP_AVG_File['indicator_code'] = '813'
IP_LOS_CORP_AVG_File['metric_code'] = 'AVERAGE'
IP_LOS_CORP_AVG_File['breakdown_type_code_l1'] = 'N/A'
IP_LOS_CORP_AVG_File['breakdown_value_code_l1'] = 'N/A'
IP_LOS_CORP_AVG_File['breakdown_type_code_l2'] = 'N/A'
IP_LOS_CORP_AVG_File['breakdown_value_code_l2'] = 'N/A'
IP_LOS_CORP_AVG_File['metric_descriptor_group_code'] = ''
IP_LOS_CORP_AVG_File['metric_descriptor_code'] = ''
IP_LOS_CORP_AVG_File['missing_reason_code'] = ''
IP_LOS_CORP_AVG_File['public_metric_result'] = IP_LOS_CORP_AVG_File['metric_result']

IP_LOS_CORP_AVG_File = IP_LOS_CORP_AVG_File[['reporting_period_code',	'reporting_entity_code','reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]
IP_LOS_CORP_AVG_File.to_csv('813_agg_20' + yr + '.csv', index=False)


