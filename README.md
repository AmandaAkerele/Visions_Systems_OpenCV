import pandas as pd

# Define mapping for IMPROVEMENT_IND_CODE values
improvement_mapping = {
    '1': 'Improving',
    '2': 'No Change',
    '3': 'Weakening'
}

# Define mapping for COMPARE_IND_CODE values
compare_mapping = {
    '1': 'Above average',
    '2': 'Same as average',
    '3': 'Below average'
}

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID", "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "improvement_code", "COMPARE_IND_CODE": "compare_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Drop rows where either IMPROVEMENT_IND_CODE or COMPARE_IND_CODE is 999
EDWT_Indicator_File = EDWT_Indicator_File[(EDWT_Indicator_File['improvement_code'] != '999') & (EDWT_Indicator_File['compare_code'] != '999')]

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    if row['metric_descriptor_group_code'] == 'PerformanceTrend':
        metric_descriptor_code = improvement_mapping.get(row['improvement_code'])
    elif row['metric_descriptor_group_code'] == 'PerformanceComparison':
        metric_descriptor_code = compare_mapping.get(row['compare_code'])
    else:
        metric_descriptor_code = ''
        
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], row['metric_descriptor_group_code'], metric_descriptor_code])

stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

# Add remaining columns
stacked_df['reporting_period_code'] = 'FY20' + yr
stacked_df['reporting_entity_type_code'] = 'ORG'
stacked_df['indicator_code'] = '811'
stacked_df['metric_code'] = 'PCTL_90'
stacked_df['breakdown_type_code_l1'] = 'N/A'
stacked_df['breakdown_value_code_l1'] = 'N/A'
stacked_df['breakdown_type_code_l2'] = 'N/A'
stacked_df['breakdown_value_code_l2'] = 'N/A'
stacked_df['missing_reason_code'] = ''
stacked_df['public_metric_result'] = stacked_df['metric_result']

# Reorder columns
stacked_df = stacked_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
stacked_df.to_csv('811_agg.csv', index=False)
