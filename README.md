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

# Define mapping for INDICATOR_SUPPRESSION_CODE values
suppression_mapping = {
    '7': '',
    '2': 'S03',
    '6': 'S10',
    '901': 'S08'
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

# Convert INDICATOR_SUPPRESSION_CODE column to numeric type
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] = pd.to_numeric(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"], errors='coerce')

# Drop rows with NaN and '999' in the INDICATOR_SUPPRESSION_CODE column
EDWT_Indicators = EDWT_Indicators[(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"].notna()) & (EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] != 999)]

# Map INDICATOR_SUPPRESSION_CODE to suppression_mapping
EDWT_Indicators['missing_reason_code'] = EDWT_Indicators['INDICATOR_SUPPRESSION_CODE'].astype(str).replace(suppression_mapping)

# Create a list to store the distributed data
distributed_data = []

# List of reporting years
reporting_years = [f"FY20{x}" for x in range(18, 23)]  # FY2018 to FY2022

# Iterate through each reporting year
for period_code in reporting_years:
    period_data = stacked_df[stacked_df['reporting_period_code'] == period_code]
    
    for _, row in period_data.iterrows():
        # Add metric_result row
        metric_result_row = row.copy()
        metric_result_row['metric_descriptor_group_code'] = None
        metric_result_row['metric_descriptor_code'] = None
        
        # Set public_metric_result to None if missing_reason_code is mapped to a suppression value
        if metric_result_row['missing_reason_code'] in suppression_mapping.values():
            metric_result_row['public_metric_result'] = None
            
        distributed_data.append(metric_result_row)
        
        # Add PerformanceTrend row
        trend_row = row.copy()
        trend_row['metric_result'] = None
        trend_row['metric_descriptor_group_code'] = 'PerformanceTrend'
        
        # Set public_metric_result to None if missing_reason_code is mapped to a suppression value
        if trend_row['missing_reason_code'] in suppression_mapping.values():
            trend_row['public_metric_result'] = None
            
        distributed_data.append(trend_row)
        
        # Add PerformanceComparison row
        comparison_row = row.copy()
        comparison_row['metric_result'] = None
        comparison_row['metric_descriptor_group_code'] = 'PerformanceComparison'
        
        # Set public_metric_result to None if missing_reason_code is mapped to a suppression value
        if comparison_row['missing_reason_code'] in suppression_mapping.values():
            comparison_row['public_metric_result'] = None
            
        distributed_data.append(comparison_row)

# Convert the list of dictionaries to a dataframe
distributed_df = pd.DataFrame(distributed_data)

# Reorder columns
distributed_df = distributed_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                                 '
