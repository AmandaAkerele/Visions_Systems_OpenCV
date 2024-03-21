# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result"}, inplace=True)

for ind in EDWT_Indicator_File.index:
    EDWT_Indicator_File["metric_result"][ind] = round_half_up((EDWT_Indicator_File["metric_result"][ind]), 1)

EDWT_Indicator_File['reporting_period_code'] = 'FY20' + yr
EDWT_Indicator_File['reporting_entity_type_code'] = 'ORG'
EDWT_Indicator_File['indicator_code'] = '811'
EDWT_Indicator_File['metric_code'] = 'PCTL_90'
EDWT_Indicator_File['breakdown_type_code_l1'] = 'N/A'
EDWT_Indicator_File['breakdown_value_code_l1'] = 'N/A'
EDWT_Indicator_File['breakdown_type_code_l2'] = 'N/A'
EDWT_Indicator_File['breakdown_value_code_l2'] = 'N/A'
EDWT_Indicator_File['metric_descriptor_group_code'] = ''
EDWT_Indicator_File['metric_descriptor_code'] = ''
EDWT_Indicator_File['missing_reason_code'] = ''
EDWT_Indicator_File['public_metric_result'] = EDWT_Indicator_File['metric_result']

# Reorder columns
EDWT_Indicator_File = EDWT_Indicator_File[['reporting_period_code',	'reporting_entity_code','reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
# EDWT_Indicator_File.to_csv('813_agg.csv', index=False)
