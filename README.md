import pandas as pd
import numpy as np
from scipy.stats import expon

# Define mapping dictionaries
improvement_mapping = {'1': 'Improving', '2': 'NoChange', '3': 'Weaken'}
compare_mapping = {'1': 'Above', '2': 'Same', '3': 'Below'}
suppression_mapping = {'7': '', '2': 'S03', '3': 'M02', '6': 'S10', '901': 'S08'}

# Sample data (assuming you have TT_Spent_ED DataFrame)
# TT_Spent_ED = pd.read_csv("your_data_file.csv")

# Convert columns to numeric types and apply mappings
for col, mapping in [('COMPARE_IND_CODE', compare_mapping),
                     ('IMPROVEMENT_IND_CODE', improvement_mapping),
                     ('INDICATOR_SUPPRESSION_CODE', suppression_mapping)]:
    TT_Spent_ED[col] = pd.to_numeric(TT_Spent_ED[col], errors='coerce')
    TT_Spent_ED[f'{col.lower()}_descriptor_code'] = TT_Spent_ED[col].astype(str).replace(mapping)

def generate_data_for_year(year):
    TT_Spent_ED_File = TT_Spent_ED[["ORGANIZATION_ID", "FISCAL_YEAR_WH_ID", 
                                    "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE", 
                                    "INDICATOR_SUPPRESSION_CODE"]]
    TT_Spent_ED_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", 
                                     "FISCAL_YEAR_WH_ID": "reporting_period_code"}, inplace=True)

    np.random.seed(0)
    scale_param = 30
    size = len(TT_Spent_ED_File)

    random_data = expon.ppf(np.random.rand(size), scale=scale_param)
    random_data_shifted = random_data + 1

    TT_Spent_ED_File['metric_result'] = random_data_shifted.round(1)
    TT_Spent_ED_File.dropna(subset=['metric_result'], inplace=True)

    stacked_data = []
    for index, row in TT_Spent_ED_File.iterrows():
        if row['INDICATOR_SUPPRESSION_CODE'] != '999':
            reporting_entity_code = row['reporting_entity_code']
            reporting_period_code = 'FY20' + str(row['reporting_period_code'])
            metric_result = row['metric_result']

            # For Row 1
            if row['INDICATOR_SUPPRESSION_CODE'] not in ['S03', 'S10', 'M02', 'S08']:
                stacked_data.append([reporting_entity_code, reporting_period_code, metric_result, '', '', row['INDICATOR_SUPPRESSION_CODE'], metric_result])
            else:
                stacked_data.append([reporting_entity_code, reporting_period_code, metric_result, '', '', row['INDICATOR_SUPPRESSION_CODE'], ''])
            
            # For Row 2
            if row['IMPROVEMENT_IND_CODE'] != '999':
                stacked_data.append([reporting_entity_code, reporting_period_code, '', 'PerformanceTrend', row['IMPROVEMENT_IND_CODE'], '', ''])
            
            # For Row 3
            if row['COMPARE_IND_CODE'] != '999':
                stacked_data.append([reporting_entity_code, reporting_period_code, '', 'PerformanceComparison', row['COMPARE_IND_CODE'], '', ''])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'reporting_period_code', 'metric_result', 
                                                     'metric_descriptor_group_code', 'metric_descriptor_code', 
                                                     'missing_reason_code', 'public_metric_result'])

    stacked_df['reporting_period_code'] = 'FY20' + stacked_df['reporting_period_code'].astype(str)
    stacked_df['reporting_entity_type_code'] = 'ORG'
    stacked_df['indicator_code'] = '810'
    stacked_df['metric_code'] = 'PCTL_90'
    stacked_df['breakdown_type_code_l1'] = 'N/A'
    stacked_df['breakdown_value_code_l1'] = 'N/A'
    stacked_df['breakdown_type_code_l2'] = 'N/A'
    stacked_df['breakdown_value_code_l2'] = 'N/A'

    stacked_df = stacked_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', 
                             'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 
                             'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', 
                             'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', 
                             'public_metric_result']]

    return stacked_df

# Generate data for each year from FY2018 to FY2022
all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

# Write to CSV
all_years_data.to_csv('fiscal_810_agg.csv', index=False)
