solve this error 
AssertionError                            Traceback (most recent call last)
~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in _finalize_columns_and_data(content, columns, dtype)
    933     try:
--> 934         columns = _validate_or_indexify_columns(contents, columns)
    935     except AssertionError as err:

~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in _validate_or_indexify_columns(content, columns)
    980             # caller's responsibility to check for this...
--> 981             raise AssertionError(
    982                 f"{len(columns)} columns passed, passed data had "

AssertionError: 5 columns passed, passed data had 4 columns

The above exception was the direct cause of the following exception:

ValueError                                Traceback (most recent call last)
/tmp/ipykernel_369/2052094697.py in <cell line: 93>()
     91 
     92 # Generate data for each year from FY2018 to FY2022
---> 93 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])
     94 
     95 # Write to CSV

/tmp/ipykernel_369/2052094697.py in <listcomp>(.0)
     91 
     92 # Generate data for each year from FY2018 to FY2022
---> 93 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])
     94 
     95 # Write to CSV

/tmp/ipykernel_369/2052094697.py in generate_data_for_year(year)
     68         stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceComparison', row['compare_descriptor_code']])
     69 
---> 70     stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code'])
     71 
     72     # Add remaining columns

~/.local/lib/python3.10/site-packages/pandas/core/frame.py in __init__(self, data, index, columns, dtype, copy)
    780                     if columns is not None:
    781                         columns = ensure_index(columns)
--> 782                     arrays, columns, index = nested_data_to_arrays(
    783                         # error: Argument 3 to "nested_data_to_arrays" has incompatible
    784                         # type "Optional[Collection[Any]]"; expected "Optional[Index]"

~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in nested_data_to_arrays(data, columns, index, dtype)
    496         columns = ensure_index(data[0]._fields)
    497 
--> 498     arrays, columns = to_arrays(data, columns, dtype=dtype)
    499     columns = ensure_index(columns)
    500 

~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in to_arrays(data, columns, dtype)
    838         arr = _list_to_arrays(data)
    839 
--> 840     content, columns = _finalize_columns_and_data(arr, columns, dtype)
    841     return content, columns
    842 

~/.local/lib/python3.10/site-packages/pandas/core/internals/construction.py in _finalize_columns_and_data(content, columns, dtype)
    935     except AssertionError as err:
    936         # GH#26429 do not raise user-facing AssertionError
--> 937         raise ValueError(err) from err
    938 
    939     if len(contents) and contents[0].dtype == np.object_:

ValueError: 5 columns passed, passed data had 4 columns


code is below 

import pandas as pd
import numpy as np
from scipy.stats import expon

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

# Define a function to generate data for a specific year
def generate_data_for_year(year):
    # Create file for shallow slice pilot
    # Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
    EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID", "improvement_descriptor_code", "compare_descriptor_code", "missing_reason_code"]]
    EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code"}, inplace=True)

    # Generate random data for metric_result based on the 90th percentile using exponential distribution
    np.random.seed(0)  # Setting seed for reproducibility
    scale_param = 5 # Scale parameter for exponential distribution
    size = len(EDWT_Indicator_File)

    # Generate random data based on the 90th percentile
    random_data = expon.ppf(np.random.rand(size), scale=scale_param)

    # Shift the data to ensure none of the values are zero
    random_data_shifted = random_data + 1

    # Assign the generated data to the dataframe
    EDWT_Indicator_File['metric_result'] = random_data_shifted.round(1)

    # Drop rows with NaN values in the 'metric_result' column
    EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

    # Stack the rows
    stacked_data = []
    for index, row in EDWT_Indicator_File.iterrows():
        stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceTrend', row['improvement_descriptor_code']])
        stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceComparison', row['compare_descriptor_code']])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code'])

    # Add remaining columns
    stacked_df['reporting_period_code'] = 'FY20' + str(year)
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

    return stacked_df

# Generate data for each year from FY2018 to FY2022
all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

# Write to CSV
# all_years_data.to_csv('DELETE_agg_all_years.csv', index=False)
