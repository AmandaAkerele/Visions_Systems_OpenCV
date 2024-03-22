import pandas as pd
import numpy as np
from scipy.stats import norm

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

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID", "improvement_descriptor_code", "compare_descriptor_code"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code"}, inplace=True)

# Generate random data for metric_result based on the 90th percentile
np.random.seed(0)  # Setting seed for reproducibility
mean = 100  # Mean value
std_dev = 20  # Standard deviation
size = len(EDWT_Indicator_File)

# Generate random data that corresponds to the 90th percentile
random_data = norm.ppf(np.random.rand(size), mean, std_dev)

# Ensure the generated data is non-negative
random_data = np.maximum(0, random_data)

# Assign the generated data to the dataframe
EDWT_Indicator_File['metric_result'] = random_data.round(1)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceTrend', row['improvement_descriptor_code']])
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceComparison', row['compare_descriptor_code']])

stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

# Add remaining columns
yr = "24"
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
# stacked_df.to_csv('811_agg.csv', index=False)


















# Convert INDICATOR_SUPPRESSION_CODE column to numeric type
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] = pd.to_numeric(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"], errors='coerce')

# Drop rows with NaN and '999' in the COMPARE_IND_CODE column
EDWT_Indicators = EDWT_Indicators[(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"].notna()) & (EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] != 999)]

# Check the column again to confirm if the rows with '999' are dropped
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"].value_counts()

New column 
add mapping for INDICATOR_SUPPRESSION_CODE
7: NA (The indicator value is not suppressed)
2: Data not available due to data quality issues
6: Data not reported due to incomplete data submission
901: Indicator data manually suppressed at Program Area’s request
