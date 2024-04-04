import pandas as pd
import numpy as np
from scipy.stats import expon

# Define mapping dictionaries
improvement_mapping = {'1': 'Improving', '2': 'NoChange', '3': 'Weaken'}
compare_mapping = {'1': 'Above', '2': 'Same', '3': 'Below'}
suppression_mapping = {'7': '', '2': 'S03', '3': 'M02', '6': 'S10', '901': 'S08'}

# Convert columns to appropriate data types and apply mappings
TT_Spent_ED["COMPARE_IND_CODE"] = pd.to_numeric(TT_Spent_ED["COMPARE_IND_CODE"], errors='coerce')
TT_Spent_ED['compare_descriptor_code'] = TT_Spent_ED['COMPARE_IND_CODE'].astype(str).replace(compare_mapping)

TT_Spent_ED["IMPROVEMENT_IND_CODE"] = pd.to_numeric(TT_Spent_ED["IMPROVEMENT_IND_CODE"], errors='coerce')
TT_Spent_ED['improvement_descriptor_code'] = TT_Spent_ED['IMPROVEMENT_IND_CODE'].astype(str).replace(improvement_mapping)

TT_Spent_ED["INDICATOR_SUPPRESSION_CODE"] = pd.to_numeric(TT_Spent_ED["INDICATOR_SUPPRESSION_CODE"], errors='coerce')
TT_Spent_ED['missing_reason_code'] = TT_Spent_ED['INDICATOR_SUPPRESSION_CODE'].astype(str).replace(suppression_mapping)

# Define a function to generate data for a specific year
def generate_data_for_year(year):
    np.random.seed(0)
    scale_param = 30
    
    # Data preparation
    TT_Spent_ED_File = TT_Spent_ED[["ORGANIZATION_ID", "improvement_descriptor_code", "compare_descriptor_code", "missing_reason_code"]]
    TT_Spent_ED_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code"}, inplace=True)
    
    stacked_data = []
    
    # Generate random metric results
    size = len(TT_Spent_ED_File)
    random_data = expon.ppf(np.random.rand(size), scale=scale_param)
    random_data_shifted = random_data + 1
    TT_Spent_ED_File['metric_result'] = random_data_shifted.round(1)
    TT_Spent_ED_File.dropna(subset=['metric_result'], inplace=True)

    # Loop through each reporting entity
    for entity_code in TT_Spent_ED_File['reporting_entity_code'].unique():
        entity_data = TT_Spent_ED_File[TT_Spent_ED_File['reporting_entity_code'] == entity_code]
        
        # Add Row 1 for each entity
        for _, row in entity_data.iterrows():
            if row['missing_reason_code'] != '999':
                if row['missing_reason_code'] not in ['S03', 'S10', 'M02', 'S08']:
                    stacked_data.append([entity_code, row['metric_result'], '', '', row['missing_reason_code'], row['metric_result']])
                else:
                    stacked_data.append([entity_code, row['metric_result'], '', '', row['missing_reason_code'], ''])
            
            # Add Row 2 and Row 3 if applicable
            if row['improvement_descriptor_code'] != '999':
                stacked_data.append([entity_code, '', 'PerformanceTrend', improvement_mapping.get(row['improvement_descriptor_code'], ''), '', ''])
            if row['compare_descriptor_code'] != '999':
                stacked_data.append([entity_code, '', 'PerformanceComparison', compare_mapping.get(row['compare_descriptor_code'], ''), '', ''])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', 'public_metric_result'])

    # Add additional columns
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

# Generate data for each year from FY2018 to FY2022
all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

# Remove duplicates based on reporting_entity_code
all_years_data = all_years_data.drop_duplicates(subset=['reporting_entity_code'])

# Sort by reporting_entity_code and then by reporting_period_code
all_years_data = all_years_data.sort_values(by=['reporting_entity_code', 'reporting_period_code'])

# Write to CSV
all_years_data.to_csv('Corrected3810_agg.csv', index=False)
