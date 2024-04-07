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

period_mapping = {year: f'FY20{year}' for year in range(18, 23)}

def generate_data_for_year(year):
    EDWT_Indicators_File = EDWT_Indicators[["FISCAL_YEAR_WH_ID", "ORGANIZATION_ID", "improvement_descriptor_code", "compare_descriptor_code", "missing_reason_code"]]
    EDWT_Indicators_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", 
                                         "FISCAL_YEAR_WH_ID": "reporting_period_code"}, inplace=True)
    
    np.random.seed(0)
    scale_param = 30
    size = len(EDWT_Indicators_File)

    random_data = expon.ppf(np.random.rand(size), scale=scale_param)
    random_data_shifted = random_data + 1

    EDWT_Indicators_File['metric_result'] = random_data_shifted.round(1)
    EDWT_Indicators_File.dropna(subset=['metric_result'], inplace=True)

    stacked_data = []
    for index, row in EDWT_Indicators_File.iterrows():
        if row.get('INDICATOR_SUPPRESSION_CODE') != '999':
            reporting_entity_code = row['reporting_entity_code']
            reporting_period_code = period_mapping[row['reporting_period_code']]
            metric_result = row['metric_result']

            # For Row 1
            if row['missing_reason_code'] not in ['S03', 'S10', 'M02', 'S08']:
                stacked_data.append([reporting_period_code, reporting_entity_code, metric_result, '', '', row['missing_reason_code'], metric_result])
            else:
                stacked_data.append([reporting_period_code, reporting_entity_code, metric_result, '', '', row['missing_reason_code'], ''])
            
            # For Row 2
            if row['improvement_descriptor_code'] != '999':
                stacked_data.append([reporting_period_code, reporting_entity_code, '', 'PerformanceTrend', row['improvement_descriptor_code'], '', ''])
            
            # For Row 3
            if row['compare_descriptor_code'] != '999':
                stacked_data.append([reporting_period_code, reporting_entity_code, '', 'PerformanceComparison', row['compare_descriptor_code'], '', ''])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_period_code', 'reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', 'public_metric_result'])

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

# Generate data for each year from FY2018 to FY2022
all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

# Write to CSV
all_years_data.to_csv('fiscal40_810_agg.csv', index=False)
