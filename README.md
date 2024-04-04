import pandas as pd
import numpy as np
from scipy.stats import expon

# Create file for shallow slice pilot
# Indicator: Total Time Spent in Emergency Department for Admitted Patients (90% Spent Less, in Hours)

# Define mapping for IMPROVEMENT_IND_CODE values
improvement_mapping = {
    '1': 'Improving',
    '2': 'NoChange',
    '3': 'Weaken'
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
    '3': 'M02',
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

# Define a function to generate data for a specific year
def generate_data_for_year(year):
    EDWT_Indicators_File = EDWT_Indicators[["ORGANIZATION_ID", "improvement_descriptor_code", "compare_descriptor_code", "missing_reason_code"]]
    EDWT_Indicators_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code"}, inplace=True)

    np.random.seed(0)
    scale_param = 30
    size = len(EDWT_Indicators_File)

    random_data = expon.ppf(np.random.rand(size), scale=scale_param)
    random_data_shifted = random_data + 1

    EDWT_Indicators_File['metric_result'] = random_data_shifted.round(1)
    EDWT_Indicators_File.dropna(subset=['metric_result'], inplace=True)

    stacked_data = []
    for reporting_entity in EDWT_Indicators_File['reporting_entity_code'].unique():
        reporting_entity_data = EDWT_Indicators_File[EDWT_Indicators_File['reporting_entity_code'] == reporting_entity]
        
        if reporting_entity_data['missing_reason_code'].iloc[0] != '999':
            # For Row 1
            if reporting_entity_data['missing_reason_code'].iloc[0] not in ['S03', 'S10', 'M02', 'S08']:
                stacked_data.append([reporting_entity, reporting_entity_data['metric_result'].iloc[0], '', '', reporting_entity_data['missing_reason_code'].iloc[0], reporting_entity_data['metric_result'].iloc[0]])
            else:
                stacked_data.append([reporting_entity, reporting_entity_data['metric_result'].iloc[0], '', '', reporting_entity_data['missing_reason_code'].iloc[0], ''])
            
            # For Row 2
            if reporting_entity_data['improvement_descriptor_code'].iloc[0] != '999':
                stacked_data.append([reporting_entity, '', 'PerformanceTrend', reporting_entity_data['improvement_descriptor_code'].iloc[0], '', ''])
            
            # For Row 3
            if reporting_entity_data['compare_descriptor_code'].iloc[0] != '999':
                stacked_data.append([reporting_entity, '', 'PerformanceComparison', reporting_entity_data['compare_descriptor_code'].iloc[0], '', ''])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', 'public_metric_result'])

    stacked_df['reporting_period_code'] = 'FY20' + str(year)
    stacked_df['reporting_entity_type_code'] = 'ORG'
    stacked_df['indicator_code'] = '810'
    stacked_df['metric_code'] = 'PCTL_90'
    stacked_df['breakdown_type_code_l1'] = 'N/A'
    stacked_df['breakdown_value_code_l1'] = 'N/A'
    stacked_df['breakdown_type_code_l2'] = 'N/A'
    stacked_df['breakdown_value_code_l2'] = 'N/A'

    stacked_df = stacked_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                        'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                       'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                       'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

    return stacked_df

# Generate data for each year from FY2018 to FY2023
all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 24)])

# Remove duplicates based on reporting_period_code and reporting_entity_code
all_years_data = all_years_data.drop_duplicates(subset=['reporting_period_code', 'reporting_entity_code'])

# Write to CSV
all_years_data.to_csv('Correction2_810_agg.csv', index=False)



