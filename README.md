import pandas as pd

# Sample data
# Assuming EDWT_Indicators is your original dataframe

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

# Convert COMPARE_IND_CODE column to numeric type
EDWT_Indicators["COMPARE_IND_CODE"] = pd.to_numeric(EDWT_Indicators["COMPARE_IND_CODE"], errors='coerce')

# Drop rows with NaN and '999' in the COMPARE_IND_CODE column
EDWT_Indicators = EDWT_Indicators[(EDWT_Indicators["COMPARE_IND_CODE"].notna()) & (EDWT_Indicators["COMPARE_IND_CODE"] != 999)]

# Map COMPARE_IND_CODE to compare_mapping
EDWT_Indicators['compare_descriptor_code'] = EDWT_Indicators['COMPARE_IND_CODE'].astype(str).replace(compare_mapping)

# Convert IMPROVEMENT_IND_CODE column to numeric type
EDWT_Indicators["IMPROVEMENT_IND_CODE"] = pd.to_numeric(EDWT_Indicators["IMPROVEMENT_IND_CODE"], errors='coerce')

# Drop rows with NaN and '999' in the IMPROVEMENT_IND_CODE column
EDWT_Indicators = EDWT_Indicators[(EDWT_Indicators["IMPROVEMENT_IND_CODE"].notna()) & (EDWT_Indicators["IMPROVEMENT_IND_CODE"] != 999)]

# Map IMPROVEMENT_IND_CODE to improvement_mapping
EDWT_Indicators['improvement_descriptor_code'] = EDWT_Indicators['IMPROVEMENT_IND_CODE'].astype(str).replace(improvement_mapping)

# Create a list to store the distributed data
distributed_data = []

# Iterate through each unique reporting_period_code
for period_code in stacked_df['reporting_period_code'].unique():
    period_data = stacked_df[stacked_df['reporting_period_code'] == period_code]
    
    for _, row in period_data.iterrows():
        # Add metric_result row
        metric_result_row = row.copy()
        metric_result_row['metric_descriptor_group_code'] = None
        metric_result_row['metric_descriptor_code'] = None
        distributed_data.append(metric_result_row)
        
        # Add PerformanceTrend row
        trend_row = row.copy()
        trend_row['metric_result'] = None
        trend_row['metric_descriptor_group_code'] = 'PerformanceTrend'
        trend_row['metric_descriptor_code'] = row['improvement_descriptor_code']
        distributed_data.append(trend_row)
        
        # Add PerformanceComparison row
        comparison_row = row.copy()
        comparison_row['metric_result'] = None
        comparison_row['metric_descriptor_group_code'] = 'PerformanceComparison'
        comparison_row['metric_descriptor_code'] = row['compare_descriptor_code']
        distributed_data.append(comparison_row)

# Convert the list of dictionaries to a dataframe
distributed_df = pd.DataFrame(distributed_data)

# Reorder columns
distributed_df = distributed_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                                 'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', \
                                 'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', \
                                 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', \
                                 'public_metric_result']]

# Sort by reporting_period_code
distributed_df = distributed_df.sort_values(by=['reporting_period_code', 'reporting_entity_code'])

# Write to CSV
distributed_df.to_csv('DELETE_agg_distributed.csv', index=False)
