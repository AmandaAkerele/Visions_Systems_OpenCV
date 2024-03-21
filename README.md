import pandas as pd

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "metric_descriptor_group_code", "COMPARE_IND_CODE": "metric_descriptor_group_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Define mapping for IMPROVEMENT_IND_CODE values
improvement_mapping = {
    '001': 'Improving',
    '002': 'No Change'
}

# Define mapping for COMPARE_IND_CODE values
compare_mapping = {
    'MAP1': 'First Map',
    'MAP2': 'Second Map'
}

# Create copies of the DataFrame for each descriptor group
improvement_df = EDWT_Indicator_File.copy()
compare_df = EDWT_Indicator_File.copy()

# Apply mappings for each descriptor group
improvement_df['metric_descriptor_code'] = improvement_df['metric_descriptor_group_code'].map(improvement_mapping)
compare_df['metric_descriptor_code'] = compare_df['metric_descriptor_group_code'].map(compare_mapping)

# Concatenate the DataFrames
stacked_EDWT_Indicator_File = pd.concat([improvement_df, compare_df], ignore_index=True)

# Remove duplicate rows
stacked_EDWT_Indicator_File.drop_duplicates(inplace=True)

# Rest of your code...
stacked_EDWT_Indicator_File['reporting_period_code'] = 'FY20' + yr
stacked_EDWT_Indicator_File['reporting_entity_type_code'] = 'ORG'
stacked_EDWT_Indicator_File['indicator_code'] = '811'
stacked_EDWT_Indicator_File['metric_code'] = 'PCTL_90'
stacked_EDWT_Indicator_File['breakdown_type_code_l1'] = 'N/A'
stacked_EDWT_Indicator_File['breakdown_value_code_l1'] = 'N/A'
stacked_EDWT_Indicator_File['breakdown_type_code_l2'] = 'N/A'
stacked_EDWT_Indicator_File['breakdown_value_code_l2'] = 'N/A'
stacked_EDWT_Indicator_File['missing_reason_code'] = ''
stacked_EDWT_Indicator_File['public_metric_result'] = stacked_EDWT_Indicator_File['metric_result']

# Reorder columns
stacked_EDWT_Indicator_File = stacked_EDWT_Indicator_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
# stacked_EDWT_Indicator_File.to_csv('811_agg.csv', index=False)
