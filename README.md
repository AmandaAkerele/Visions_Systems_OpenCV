# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Map metric_descriptor_group_code based on specific strings
EDWT_Indicator_File['metric_descriptor_group_code'] = np.where(
    EDWT_Indicator_File['metric_descriptor_group_code'].str.contains('IMPROVEMENT_IND_CODE', case=False, na=False),
    'IMPROVEMENT_IND_CODE',
    np.where(
        EDWT_Indicator_File['metric_descriptor_group_code'].str.contains('COMPARE_IND_CODE', case=False, na=False),
        'COMPARE_IND_CODE',
        ''
    )
)

# Map metric_descriptor_code based on metric_descriptor_group_code
EDWT_Indicator_File['metric_descriptor_code'] = np.where(
    EDWT_Indicator_File['metric_descriptor_group_code'] == 'IMPROVEMENT_IND_CODE',
    'WRITE UNDER',
    np.where(
        EDWT_Indicator_File['metric_descriptor_group_code'] == 'COMPARE_IND_CODE',
        'MAPI ON',
        ''
    )
)

# Rest of your code...
EDWT_Indicator_File['reporting_period_code'] = 'FY20' + yr
EDWT_Indicator_File['reporting_entity_type_code'] = 'ORG'
EDWT_Indicator_File['indicator_code'] = '811'
EDWT_Indicator_File['metric_code'] = 'PCTL_90'
EDWT_Indicator_File['breakdown_type_code_l1'] = 'N/A'
EDWT_Indicator_File['breakdown_value_code_l1'] = 'N/A'
EDWT_Indicator_File['breakdown_type_code_l2'] = 'N/A'
EDWT_Indicator_File['breakdown_value_code_l2'] = 'N/A'
EDWT_Indicator_File['missing_reason_code'] = ''
EDWT_Indicator_File['public_metric_result'] = EDWT_Indicator_File['metric_result']

# Reorder columns
EDWT_Indicator_File = EDWT_Indicator_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
# EDWT_Indicator_File.to_csv('813_agg.csv', index=False)
