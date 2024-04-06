import pandas as pd
import numpy as np
from scipy.stats import expon

improvement_mapping = {
    '1.0': 'Improving',
    '2.0': 'NoChange',
    '3.0': 'Weaken'
}

# Define mapping for COMPARE_IND_CODE values
compare_mapping = {
    '1.0': 'Above',
    '2.0': 'Same',
    '3.0': 'Below'
}

# Define mapping for INDICATOR_SUPPRESSION_CODE values 
suppression_mapping = {
    '7.0': '',
    '2.0': 'S03',
    '3.0': 'M02',
    '6.0': 'S10',
    '901.0': 'S08'
}

# Define mapping for missing_reason_code values 
missing_reason_mapping = {
    'S03': 'DataIssue',
    'S10': 'SystemIssue',
    'M02': 'MethodologyChange',
    'S08': 'Other'
}

period_mapping = {year: f'FY20{year}' for year in range(18, 23)}

# Convert columns to numeric types and apply mappings
for col, mapping in [('COMPARE_IND_CODE', compare_mapping),
                     ('IMPROVEMENT_IND_CODE', improvement_mapping),
                     ('INDICATOR_SUPPRESSION_CODE', suppression_mapping)]:  
    TT_Spent_ED[col] = pd.to_numeric(TT_Spent_ED[col], errors='coerce')
    TT_Spent_ED[f'{col.lower()}_descriptor_code'] = TT_Spent_ED[col].map(mapping).fillna('')

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
        if row['INDICATOR_SUPPRESSION_CODE'] != 999:
            reporting_entity_code = row['reporting_entity_code']
            reporting_period_code = period_mapping[row['reporting_period_code']]
            metric_result = row['metric_result']

            missing_reason = row['INDICATOR_SUPPRESSION_CODE_descriptor_code']
            if missing_reason in missing_reason_mapping:
                missing_reason = missing_reason_mapping[missing_reason]

            # For Row 1
            if row['INDICATOR_SUPPRESSION_CODE'] not in ['S03', 'S10', 'M02', 'S08']:
                stacked_data.append([reporting_entity_code, reporting_period_code, metric_result, '', '', row['INDICATOR_SUPPRESSION_CODE'], metric_result])
            else:
                stacked_data.append([reporting_entity_code, reporting_period_code, metric_result, '', '', row['INDICATOR_SUPPRESSION_CODE'], missing_reason])
            
            # For Row 2
            if row['IMPROVEMENT_IND_CODE'] != 999:
                stacked_data.append([reporting_entity_code, reporting_period_code, '', 'PerformanceTrend', improvement_mapping[str(row['IMPROVEMENT_IND_CODE'])], '', ''])
            
            # For Row 3
            if row['COMPARE_IND_CODE'] != 999:
                stacked_data.append([reporting_entity_code, reporting_period_code, '', 'PerformanceComparison', compare_mapping[str(row['COMPARE_IND_CODE'])], '', ''])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'reporting_period_code', 'metric_result', 
                                                     'metric_descriptor_group_code', 'metric_descriptor_code', 
                                                     'missing_reason_code', 'public_metric_result'])

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
all_years_data.to_csv('fiscal3laugh    _810_agg.csv', index=False)



fine 



import pandas as pd
import numpy as np
from scipy.stats import expon

improvement_mapping = {
    '1.0': 'Improving',
    '2.0': 'NoChange',
    '3.0': 'Weaken'
}

# Define mapping for COMPARE_IND_CODE values
compare_mapping = {
    '1.0': 'Above',
    '2.0': 'Same',
    '3.0': 'Below'
}

# Define mapping for INDICATOR_SUPPRESSION_CODE values 
suppression_mapping = {
    '7.0': '',
    '2.0': 'S03',
    '3.0': 'M02',
    '6.0': 'S10',
    '901.0': 'S08'
}

period_mapping = {year: f'FY20{year}' for year in range(18, 23)}

# Convert columns to numeric types and apply mappings
for col, mapping in [('COMPARE_IND_CODE', compare_mapping),
                     ('IMPROVEMENT_IND_CODE', improvement_mapping),
                     ('INDICATOR_SUPPRESSION_CODE', suppression_mapping)]:
    TT_Spent_ED[col] = pd.to_numeric(TT_Spent_ED[col], errors='coerce')
    TT_Spent_ED[f'{col.lower()}_descriptor_code'] = TT_Spent_ED[col].map(mapping).fillna('')

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
        if row['INDICATOR_SUPPRESSION_CODE'] != 999:
            reporting_entity_code = row['reporting_entity_code']
            reporting_period_code = period_mapping[row['reporting_period_code']]
            metric_result = row['metric_result']
            
            # Mapping for missing_reason_code
            missing_reason_code = suppression_mapping.get(str(row['INDICATOR_SUPPRESSION_CODE']), '')
            
            # For Row 1
            if row['INDICATOR_SUPPRESSION_CODE'] not in ['S03', 'S10', 'M02', 'S08']:
                stacked_data.append([reporting_entity_code, reporting_period_code, metric_result, '', '', row['INDICATOR_SUPPRESSION_CODE'], metric_result])
            else:
                stacked_data.append([reporting_entity_code, reporting_period_code, metric_result, '', '', row['INDICATOR_SUPPRESSION_CODE'], ''])
            
            # For Row 2
            if row['IMPROVEMENT_IND_CODE'] != 999:
                stacked_data.append([reporting_entity_code, reporting_period_code, '', 'PerformanceTrend', improvement_mapping[str(row['IMPROVEMENT_IND_CODE'])], '', ''])
            
            # For Row 3
            if row['COMPARE_IND_CODE'] != 999:
                stacked_data.append([reporting_entity_code, reporting_period_code, '', 'PerformanceComparison', compare_mapping[str(row['COMPARE_IND_CODE'])], '', ''])
            
            # Append the missing_reason_code
            stacked_data[-1][-2] = missing_reason_code

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'reporting_period_code', 'metric_result', 
                                                     'metric_descriptor_group_code', 'metric_descriptor_code', 
                                                     'missing_reason_code', 'public_metric_result'])

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
all_years_data.to_csv('fiscal3_810_agg.csv', index=False)
