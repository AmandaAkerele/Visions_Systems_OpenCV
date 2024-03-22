# Create a new dataframe to store the distributed data
distributed_data = pd.DataFrame(columns=stacked_df.columns)

# Iterate through each row in stacked_df
for index, row in stacked_df.iterrows():
    # Create a new dictionary for the metric_result row
    metric_result_row = row.copy()
    metric_result_row['metric_descriptor_group_code'] = 'PerformanceTrend'
    metric_result_row = metric_result_row[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                                            'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', \
                                            'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', \
                                            'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', \
                                            'public_metric_result']]
    
    # Create a new dictionary for the metric_descriptor_group_code row
    metric_descriptor_group_row = row.copy()
    metric_descriptor_group_row['metric_result'] = None
    metric_descriptor_group_row['metric_descriptor_code'] = None
    metric_descriptor_group_row = metric_descriptor_group_row[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                                                                'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', \
                                                                'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', \
                                                                'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', \
                                                                'public_metric_result']]
    
    # Create a new dictionary for the metric_descriptor_code row
    metric_descriptor_code_row = row.copy()
    metric_descriptor_code_row['metric_result'] = None
    metric_descriptor_code_row['metric_descriptor_group_code'] = None
    metric_descriptor_code_row = metric_descriptor_code_row[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                                                              'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', \
                                                              'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', \
                                                              'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', \
                                                              'public_metric_result']]
    
    # Append the new rows to the distributed_data dataframe
    distributed_data = distributed_data.append(metric_result_row, ignore_index=True)
    distributed_data = distributed_data.append(metric_descriptor_group_row, ignore_index=True)
    distributed_data = distributed_data.append(metric_descriptor_code_row, ignore_index=True)

# Write to CSV
# distributed_data.to_csv('811_agg_distributed.csv', index=False)
