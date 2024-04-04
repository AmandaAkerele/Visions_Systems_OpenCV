import pandas as pd
import numpy as np
from scipy.stats import expon

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
TT_Spent_ED["COMPARE_IND_CODE"] = pd.to_numeric(TT_Spent_ED["COMPARE_IND_CODE"], errors='coerce')
TT_Spent_ED['compare_descriptor_code'] = TT_Spent_ED['COMPARE_IND_CODE'].astype(str).replace(compare_mapping)

# Convert IMPROVEMENT_IND_CODE column to numeric type
TT_Spent_ED["IMPROVEMENT_IND_CODE"] = pd.to_numeric(TT_Spent_ED["IMPROVEMENT_IND_CODE"], errors='coerce')
TT_Spent_ED['improvement_descriptor_code'] = TT_Spent_ED['IMPROVEMENT_IND_CODE'].astype(str).replace(improvement_mapping)

# Convert INDICATOR_SUPPRESSION_CODE column to numeric type
TT_Spent_ED["INDICATOR_SUPPRESSION_CODE"] = pd.to_numeric(TT_Spent_ED["INDICATOR_SUPPRESSION_CODE"], errors='coerce')
TT_Spent_ED['missing_reason_code'] = TT_Spent_ED['INDICATOR_SUPPRESSION_CODE'].astype(str).replace(suppression_mapping)

# Define a function to generate data for a specific year
def generate_data_for_year(year):
    TT_Spent_ED_File = TT_Spent_ED[["ORGANIZATION_ID", "improvement_descriptor_code", "compare_descriptor_code", "missing_reason_code"]]
    TT_Spent_ED_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code"}, inplace=True)

    np.random.seed(0)
    scale_param = 30
    size = len(TT_Spent_ED_File)

    random_data = expon.ppf(np.random.rand(size), scale=scale_param)
    random_data_shifted = random_data + 1

    TT_Spent_ED_File['metric_result'] = random_data_shifted.round(1)
    TT_Spent_ED_File.dropna(subset=['metric_result'], inplace=True)

    stacked_data = []
    
    # Group by reporting_entity_code to ensure all rows are generated for each unique reporting_entity_code
    for reporting_entity, group_data in TT_Spent_ED_File.groupby('reporting_entity_code'):
        for index, row in group_data.iterrows():
            if row['missing_reason_code'] != '999':
                # For Row 1
                if row['missing_reason_code'] not in ['S03', 'S10', 'M02', 'S08']:
                    stacked_data.append([reporting_entity, row['metric_result'], '', '', row['missing_reason_code'], row['metric_result']])
                else:
                    stacked_data.append([reporting_entity, row['metric_result'], '', '', row['missing_reason_code'], ''])

                # For Row 2
                if row['improvement_descriptor_code'] != '999':
                    stacked_data.append([reporting_entity, '', 'PerformanceTrend', improvement_mapping.get(row['improvement_descriptor_code'], ''), '', ''])

                # For Row 3
                if row['compare_descriptor_code'] != '999':
                    stacked_data.append([reporting_entity, '', 'PerformanceComparison', compare_mapping.get(row['compare_descriptor_code'], ''), '', ''])

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
