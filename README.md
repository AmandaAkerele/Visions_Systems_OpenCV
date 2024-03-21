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

# Stack the metric_descriptor_group_code column
stacked_EDWT_Indicator_File = EDWT_Indicator_File.melt(id_vars=['reporting_entity_code', 'metric_result', 'reporting_period_code', 'reporting_entity_type_code', 'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', 'breakdown_value_code_l2', 'missing_reason_code', 'public_metric_result'],
                                                       value_vars=['metric_descriptor_group_code'],
                                                       var_name='descriptor_type', value_name='metric_descriptor_group_code')

# Remove duplicate rows
stacked_EDWT_Indicator_File.drop_duplicates(inplace=True)

# Reset index
stacked_EDWT_Indicator_File.reset_index(drop=True, inplace=True)

# Map metric_descriptor_code based on metric_descriptor_group_code
stacked_EDWT_Indicator_File['metric_descriptor_code'] = stacked_EDWT_Indicator_File.apply(
    lambda row: improvement_mapping.get(row['metric_descriptor_group_code'], '') if row['descriptor_type'] == 'IMPROVEMENT_IND_CODE' else compare_mapping.get(row['metric_descriptor_group_code'], ''),
    axis=1
)

# Rest of your code...
stacked_EDWT_Indicator_File['public_metric_result'] = stacked_EDWT_Indicator_File['metric_result']

# Reorder columns
stacked_EDWT_Indicator_File = stacked_EDWT_Indicator_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
# stacked_EDWT_Indicator_File.to_csv('811_agg.csv', index=False)
