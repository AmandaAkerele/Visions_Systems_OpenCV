# Duplicate each row in the dataframe twice
duplicated_data = pd.concat([stacked_df] * 2, ignore_index=True)

# Update the metric_descriptor_group_code for the second set of duplicated rows
duplicated_data.loc[len(stacked_df):, 'metric_descriptor_group_code'] = 'PerformanceComparison'

# Rearrange columns to match the desired structure
duplicated_data = duplicated_data[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                                   'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', \
                                   'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', \
                                   'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', \
                                   'public_metric_result']]

# Write to CSV
# duplicated_data.to_csv('811_agg_distributed.csv', index=False)
