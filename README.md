import pandas as pd
import numpy as np

# Sample data (replace this with your actual data)
data = {
    'ORGANIZATION_ID': [1, 2, 3],
    'IMPROVEMENT_IND_CODE': ['1', '2', '3'],
    'COMPARE_IND_CODE': ['1', '2', '3'],
    'INDICATOR_SUPPRESSION_CODE': ['7', '2', '901']
}
EDWT_Indicators = pd.DataFrame(data)

# Define mapping for IMPROVEMENT_IND_CODE values
improvement_mapping = {
    '1': 'Improving',
    '2': 'No Change',
    '3': 'Weakening'
}

# Define mapping for COMPARE_IND_CODE values
compare_mapping = {
    '1': 'Above',
    '2': 'Same',
    '3': 'Below'
}

# Define mapping for INDICATOR_SUPPRESSION_CODE values 
suppression_mapping = {
    '7': '',
    '2': 'S03',
    '3': 'S10',
    '6': 'S10',
    '901': 'S08'
}

# Convert COMPARE_IND_CODE column to numeric type
EDWT_Indicators["COMPARE_IND_CODE"] = pd.to_numeric(EDWT_Indicators["COMPARE_IND_CODE"], errors='coerce')
EDWT_Indicators['compare_descriptor_code'] = EDWT_Indicators['COMPARE_IND_CODE'].astype(str).replace(compare_mapping)

# Convert IMPROVEMENT_IND_CODE column to numeric type
EDWT_Indicators["IMPROVEMENT_IND_CODE"] = pd.to_numeric(EDWT_Indicators["IMPROVEMENT_IND_CODE"], errors='coerce')
EDWT_Indicators['improvement_descriptor_code'] = EDWT_Indicators['IMPROVEMENT_IND_CODE'].astype(str).replace(improvement_mapping)

# Convert INDICATOR_SUPPRESSION_CODE column to numeric type
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] = pd.to_numeric(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"], errors='coerce')
EDWT_Indicators['missing_reason_code'] = EDWT_Indicators['INDICATOR_SUPPRESSION_CODE'].astype(str).replace(suppression_mapping)

# Filter out rows with '999' in metric_descriptor_code and missing_reason_code
EDWT_Indicators = EDWT_Indicators[~EDWT_Indicators['improvement_descriptor_code'].isin(['999'])]
EDWT_Indicators = EDWT_Indicators[~EDWT_Indicators['missing_reason_code'].isin(['999'])]

# Write filtered data to CSV
EDWT_Indicators.to_csv('filtered_EDWT_Indicators.csv', index=False)
