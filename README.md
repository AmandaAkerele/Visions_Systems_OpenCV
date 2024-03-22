import pandas as pd
import math
import numpy as np
import warnings

# Ignore all warnings
warnings.filterwarnings("ignore")

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

# Read data from CSV and Excel files
file_path1 = '/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/hsp_ind_organization_fact34.csv'
file_path2 = '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/DATASETS/Data Submission/hsp_ind_organization_fact34.xlsx'
EDWT_Indicators = pd.read_csv(file_path1)
TT_Spent_ED = pd.read_excel(file_path2)

# Function to round numbers half up
def round_half_up(n, decimals):
    multiplier = 10 ** decimals
    value_1 = n * multiplier
    if value_1 - math.floor(value_1) >= 0.5:
        value_2 = math.ceil(value_1)
    else:
        value_2 = math.floor(value_1)
    return (value_2 / multiplier)

# Year of data
yr = "22"

# Create file for shallow slice pilot
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID", "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "metric_descriptor_group_code", "COMPARE_IND_CODE": "metric_descriptor_group_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Rest of the code...
EDWT_Indicator_File['reporting_period_code'] = 'FY20' + yr
EDWT_Indicator_File['reporting_entity_type_code'] = 'ORG'
EDWT_Indicator_File['indicator_code'] = '811'
EDWT_Indicator_File['metric_code'] = 'PCTL_90'
EDWT_Indicator_File['breakdown_type_code_l1'] = 'N/A'
EDWT_Indicator_File['breakdown_value_code_l1'] = 'N/A'
EDWT_Indicator_File['breakdown_type_code_l2'] = 'N/A'
EDWT_Indicator_File['breakdown_value_code_l2'] = 'N/A'
EDWT_Indicator_File['metric_descriptor_code'] = ''
EDWT_Indicator_File['missing_reason_code'] = ''
EDWT_Indicator_File['public_metric_result'] = EDWT_Indicator_File['metric_result']

# Reorder columns
EDWT_Indicator_File = EDWT_Indicator_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', 'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
EDWT_Indicator_File.to_csv('811_agg.csv', index=False)
