# Create a list to store the distributed data
distributed_data = []

# Iterate through each unique reporting_period_code
for period_code in stacked_df['reporting_period_code'].unique():
    period_data = stacked_df[stacked_df['reporting_period_code'] == period_code]
    
    for _, row in period_data.iterrows():
        # Add metric_result row
        metric_result_row = row.copy()
        metric_result_row['metric_descriptor_group_code'] = 'PerformanceTrend'
        distributed_data.append(metric_result_row)
        
        # Add metric_descriptor_group_code row
        metric_descriptor_group_row = row.copy()
        metric_descriptor_group_row['metric_result'] = None
        metric_descriptor_group_row['metric_descriptor_code'] = None
        metric_descriptor_group_row['metric_descriptor_group_code'] = 'PerformanceComparison'
        distributed_data.append(metric_descriptor_group_row)
        
        # Add metric_descriptor_code row
        metric_descriptor_code_row = row.copy()
        metric_descriptor_code_row['metric_result'] = None
        metric_descriptor_code_row['metric_descriptor_group_code'] = None
        distributed_data.append(metric_descriptor_code_row)

# Convert the list of dictionaries to a dataframe
distributed_df = pd.DataFrame(distributed_data)

# Reorder columns
distributed_df = distributed_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                                 'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', \
                                 'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', \
                                 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', \
                                 'public_metric_result']]

# Write to CSV
# distributed_df.to_csv('811_agg_distributed.csv', index=False)
