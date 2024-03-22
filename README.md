# Create DataFrame with correct columns
stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

# Set 'missing_reason_code' separately
stacked_df['missing_reason_code'] = ''

# Add remaining columns
stacked_df['reporting_period_code'] = 'FY20' + str(year)
stacked_df['reporting_entity_type_code'] = 'ORG'
stacked_df['indicator_code'] = '811'
stacked_df['metric_code'] = 'PCTL_90'
stacked_df['breakdown_type_code_l1'] = 'N/A'
stacked_df['breakdown_value_code_l1'] = 'N/A'
stacked_df['breakdown_type_code_l2'] = 'N/A'
stacked_df['breakdown_value_code_l2'] = 'N/A'
stacked_df['public_metric_result'] = stacked_df['metric_result']

# Reorder columns
stacked_df = stacked_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                    'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                    'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]
