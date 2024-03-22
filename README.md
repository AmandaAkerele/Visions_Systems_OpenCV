import pandas as pd

# Define mapping for PerformanceTrend values
performance_trend_mapping = {
    '1': 'Improving',
    '2': 'No Change',
    '3': 'Weakening',
}

# Define mapping for PerformanceComparison values
performance_comparison_mapping = {
    '1': 'Above average',
    '2': 'Same as average',
    '3': 'Below average',

}

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "improvement_code", "COMPARE_IND_CODE": "compare_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Map IMPROVEMENT_IND_CODE to improvement_mapping
EDWT_Indicator_File['metric_descriptor_group_code'] = EDWT_Indicator_File['improvement_code'].replace(performance_trend_mapping)

# Map COMPARE_IND_CODE to compare_mapping
EDWT_Indicator_File['metric_descriptor_group_code'] = EDWT_Indicator_File['metric_descriptor_group_code'].fillna(EDWT_Indicator_File['compare_code'].replace(performance_comparison_mapping))

# Map metric_descriptor_group_code to metric_descriptor_code
EDWT_Indicator_File['metric_descriptor_code'] = EDWT_Indicator_File['metric_descriptor_group_code'].map(lambda x: performance_trend_mapping[x] if x in performance_trend_mapping else performance_comparison_mapping[x])

# Drop the original columns
EDWT_Indicator_File.drop(columns=['improvement_code', 'compare_code'], inplace=True)

# Filter out rows with metric_descriptor_code as 999
EDWT_Indicator_File = EDWT_Indicator_File[EDWT_Indicator_File['metric_descriptor_code'] != '999']

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], row['metric_descriptor_group_code'], row['metric_descriptor_code']])

stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

# Add remaining columns
yr = "22" # Define yr variable
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


solve the error below 












Code




Python 3 (ipykernel)
import pandas as pd
import math

import numpy as np

import warnings
# Ignore all warnings
warnings.filterwarnings("ignore")
file_path1= '/Data/Groups/CAD/YHS InDepth/PYTHON_PROJECT_2023/python/Amanda/hsp_ind_organization_fact34.csv'
EDWT_Indicators = pd.read_csv(file_path1)

display(EDWT_Indicators)



file_path1= '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/DATASETS/Data Submission/hsp_ind_organization_fact34.xlsx'
file_path2= '/Data/Groups/CAD/YHS InDepth/YHS In-Depth 2023 Nov Release/DATASETS/Data Submission/hsp_ind_organization_fact33.xlsx'
EDWT_Indicators = pd.read_excel(file_path1)
TT_Spent_ED = pd.read_excel(file_path2)
display(EDWT_Indicators)
display(TT_Spent_ED)

ORGANIZATION_ID	INDICATOR_CODE	FISCAL_YEAR_WH_ID	SEX_WH_ID	INDICATOR_SUPPRESSION_CODE	TOP_PERFORMER_IND_CODE	IMPROVEMENT_IND_CODE	COMPARE_IND_CODE	INDICATOR_VALUE	UPPER_CONFIDENCE_INTERVAL	LOWER_CONFIDENCE_INTERVAL	NUMERATOR_VALUE	DENOMINATOR_VALUE	FUNNEL_PLOT_X_VALUE	DATA_PERIOD_CODE	DATA_PERIOD_TYPE_CODE
0	1	34	18	3	7	999	999	999	3.9	.	.	.	.	.	18	FY
1	1	34	19	3	7	999	999	999	4.1	.	.	.	.	.	19	FY
2	1	34	20	3	7	999	999	999	3.2	.	.	.	.	.	20	FY
3	1	34	21	3	7	999	999	999	4.2	.	.	.	.	.	21	FY
4	1	34	22	3	7	999	999	999	5	.	.	.	.	.	22	FY
...	...	...	...	...	...	...	...	...	...	...	...	...	...	...	...	...
2090	99331	34	18	3	2	999	999	999	.	.	.	.	.	.	18	FY
2091	99331	34	19	3	2	999	999	999	.	.	.	.	.	.	19	FY
2092	99331	34	20	3	2	999	999	999	.	.	.	.	.	.	20	FY
2093	99331	34	21	3	2	999	999	999	.	.	.	.	.	.	21	FY
2094	99331	34	22	3	2	999	999	999	.	.	.	.	.	.	22	FY
2095 rows × 16 columns

ORGANIZATION_ID	INDICATOR_CODE	FISCAL_YEAR_WH_ID	SEX_WH_ID	INDICATOR_SUPPRESSION_CODE	TOP_PERFORMER_IND_CODE	IMPROVEMENT_IND_CODE	COMPARE_IND_CODE	INDICATOR_VALUE	UPPER_CONFIDENCE_INTERVAL	LOWER_CONFIDENCE_INTERVAL	NUMERATOR_VALUE	DENOMINATOR_VALUE	FUNNEL_PLOT_X_VALUE	DATA_PERIOD_CODE	DATA_PERIOD_TYPE_CODE
0	1	33	18	3	7	999	999	999	35.5	.	.	.	.	.	18	FY
1	1	33	19	3	7	999	999	999	38.3	.	.	.	.	.	19	FY
2	1	33	20	3	7	999	999	999	33.5	.	.	.	.	.	20	FY
3	1	33	21	3	7	999	999	999	40.7	.	.	.	.	.	21	FY
4	1	33	22	3	7	999	999	999	49	.	.	.	.	.	22	FY
...	...	...	...	...	...	...	...	...	...	...	...	...	...	...	...	...
2090	99331	33	18	3	7	999	999	999	14	.	.	.	.	.	18	FY
2091	99331	33	19	3	7	999	999	999	14.4	.	.	.	.	.	19	FY
2092	99331	33	20	3	7	999	999	999	21	.	.	.	.	.	20	FY
2093	99331	33	21	3	7	999	999	999	21.8	.	.	.	.	.	21	FY
2094	99331	33	22	3	7	999	2	2	21.1	.	.	.	.	.	22	FY
2095 rows × 16 columns

def round_half_up(n, decimals):
    multiplier = 10**decimals
    value_1 = n * multiplier
    if (round(float(value_1) % 1, 1)) >= 0.5:
        value_2 = math.ceil(value_1)
    else:
        value_2 = math.floor(value_1)
    return (value_2 / multiplier)
# year of data
yr = str(22)

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Rest of your code remains unchanged...

EDWT_Indicator_File['reporting_period_code'] = 'FY20' + yr
EDWT_Indicator_File['reporting_entity_type_code'] = 'ORG'
EDWT_Indicator_File['indicator_code'] = '811'
EDWT_Indicator_File['metric_code'] = 'PCTL_90'
EDWT_Indicator_File['breakdown_type_code_l1'] = 'N/A'
EDWT_Indicator_File['breakdown_value_code_l1'] = 'N/A'
EDWT_Indicator_File['breakdown_type_code_l2'] = 'N/A'
EDWT_Indicator_File['breakdown_value_code_l2'] = 'N/A'
EDWT_Indicator_File['metric_descriptor_group_code'] = ''
EDWT_Indicator_File['metric_descriptor_code'] = ''
EDWT_Indicator_File['missing_reason_code'] = ''
EDWT_Indicator_File['public_metric_result'] = EDWT_Indicator_File['metric_result']

# Reorder columns
EDWT_Indicator_File = EDWT_Indicator_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
# EDWT_Indicator_File.to_csv('811_agg.csv', index=False)

---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_1073/2824350669.py in <cell line: 13>()
     11 
     12 # Round the non-NaN values
---> 13 EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)
     14 
     15 # Rest of your code remains unchanged...

~/.local/lib/python3.10/site-packages/pandas/core/series.py in round(self, decimals, *args, **kwargs)
   2567         """
   2568         nv.validate_round(args, kwargs)
-> 2569         result = self._values.round(decimals)
   2570         result = self._constructor(result, index=self.index, copy=False).__finalize__(
   2571             self, method="round"

TypeError: can't multiply sequence by non-int of type 'float'
EDWT_Indicator_File 
# year of data
yr = str(22)

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE","IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result","IMPROVEMENT_IND_CODE": "metric_descriptor_group_code", "COMPARE_IND_CODE": "metric_descriptor_group_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)
GIT
# Rest of your code remains unchanged...

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
EDWT_Indicator_File = EDWT_Indicator_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
# EDWT_Indicator_File.to_csv('811_agg.csv', index=False)

EDWT_Indicator_File 
import math

def round_half_up(n, decimals):
    if math.isnan(n):
        return float('nan')  # Return NaN if input is NaN
    multiplier = 10 ** decimals
    value_1 = n * multiplier
    if value_1 - math.floor(value_1) >= 0.5:
        value_2 = math.ceil(value_1)
    else:
        value_2 = math.floor(value_1)
    return (value_2 / multiplier)


and 

import pandas as pd


  File "/tmp/ipykernel_1073/900693860.py", line 15
    and
    ^
SyntaxError: invalid syntax
    '003': 'Weakening'
}

# Define mapping for COMPARE_IND_CODE values
compare_mapping = {
    '001': 'Above average',
    '002': 'Same as average',
    '003': 'Below average'
}

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "improvement_code", "COMPARE_IND_CODE": "compare_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Map IMPROVEMENT_IND_CODE to improvement_mapping
EDWT_Indicator_File['improvement'] = EDWT_Indicator_File['improvement_code'].replace(improvement_mapping)

# Map COMPARE_IND_CODE to compare_mapping
EDWT_Indicator_File['compare'] = EDWT_Indicator_File['compare_code'].replace(compare_mapping)

# Drop the original columns
EDWT_Indicator_File.drop(columns=['improvement_code', 'compare_code'], inplace=True)

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceTrend', row['improvement']])
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceComparison', row['compare']])

stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

# Add remaining columns
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
stacked_df.to_csv('811_agg.csv', index=False)

---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_1073/2131978976.py in <cell line: 92>()
     90 
     91 # Round the non-NaN values
---> 92 EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)
     93 
     94 # Map IMPROVEMENT_IND_CODE to improvement_mapping

~/.local/lib/python3.10/site-packages/pandas/core/series.py in round(self, decimals, *args, **kwargs)
   2567         """
   2568         nv.validate_round(args, kwargs)
-> 2569         result = self._values.round(decimals)
   2570         result = self._constructor(result, index=self.index, copy=False).__finalize__(
   2571             self, method="round"

TypeError: can't multiply sequence by non-int of type 'float'
display (stacked_df)
# or 



import pandas as pd

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

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "improvement_code", "COMPARE_IND_CODE": "compare_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Map IMPROVEMENT_IND_CODE to improvement_mapping
EDWT_Indicator_File['improvement'] = EDWT_Indicator_File['improvement_code'].replace(improvement_mapping)

# Map COMPARE_IND_CODE to compare_mapping
EDWT_Indicator_File['compare'] = EDWT_Indicator_File['compare_code'].replace(compare_mapping)

# Drop the original columns
EDWT_Indicator_File.drop(columns=['improvement_code', 'compare_code'], inplace=True)

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceTrend', row['improvement']])
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceComparison', row['compare']])

stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

# Add remaining columns
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

---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_1073/2996484840.py in <cell line: 92>()
     90 
     91 # Round the non-NaN values
---> 92 EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)
     93 
     94 # Map IMPROVEMENT_IND_CODE to improvement_mapping

~/.local/lib/python3.10/site-packages/pandas/core/series.py in round(self, decimals, *args, **kwargs)
   2567         """
   2568         nv.validate_round(args, kwargs)
-> 2569         result = self._values.round(decimals)
   2570         result = self._constructor(result, index=self.index, copy=False).__finalize__(
   2571             self, method="round"

TypeError: can't multiply sequence by non-int of type 'float'
stacked_df
# Drop rows where metric_descriptor_code is 999 and metric_descriptor_group_code is PerformanceTrend
EDWT_Indicator_File = EDWT_Indicator_File[~((EDWT_Indicator_File['metric_descriptor_code'] == '999') & (EDWT_Indicator_File['metric_descriptor_group_code'] == 'PerformanceTrend'))]
---------------------------------------------------------------------------
KeyError                                  Traceback (most recent call last)
~/.local/lib/python3.10/site-packages/pandas/core/indexes/base.py in get_loc(self, key)
   3651         try:
-> 3652             return self._engine.get_loc(casted_key)
   3653         except KeyError as err:

~/.local/lib/python3.10/site-packages/pandas/_libs/index.pyx in pandas._libs.index.IndexEngine.get_loc()

~/.local/lib/python3.10/site-packages/pandas/_libs/index.pyx in pandas._libs.index.IndexEngine.get_loc()

pandas/_libs/hashtable_class_helper.pxi in pandas._libs.hashtable.PyObjectHashTable.get_item()

pandas/_libs/hashtable_class_helper.pxi in pandas._libs.hashtable.PyObjectHashTable.get_item()

KeyError: 'metric_descriptor_code'

The above exception was the direct cause of the following exception:

KeyError                                  Traceback (most recent call last)
/tmp/ipykernel_1073/2733975850.py in <cell line: 2>()
      1 # Drop rows where metric_descriptor_code is 999 and metric_descriptor_group_code is PerformanceTrend
----> 2 EDWT_Indicator_File = EDWT_Indicator_File[~((EDWT_Indicator_File['metric_descriptor_code'] == '999') & (EDWT_Indicator_File['metric_descriptor_group_code'] == 'PerformanceTrend'))]

~/.local/lib/python3.10/site-packages/pandas/core/frame.py in __getitem__(self, key)
   3759             if self.columns.nlevels > 1:
   3760                 return self._getitem_multilevel(key)
-> 3761             indexer = self.columns.get_loc(key)
   3762             if is_integer(indexer):
   3763                 indexer = [indexer]

~/.local/lib/python3.10/site-packages/pandas/core/indexes/base.py in get_loc(self, key)
   3652             return self._engine.get_loc(casted_key)
   3653         except KeyError as err:
-> 3654             raise KeyError(key) from err
   3655         except TypeError:
   3656             # If we have a listlike key, _check_indexing_error will raise

KeyError: 'metric_descriptor_code'
EDWT_Indicator_File




import pandas as pd

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

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "improvement_code", "COMPARE_IND_CODE": "compare_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Map IMPROVEMENT_IND_CODE to improvement_mapping
EDWT_Indicator_File['metric_descriptor_code'] = EDWT_Indicator_File['improvement_code'].replace(improvement_mapping)

# Map COMPARE_IND_CODE to compare_mapping
EDWT_Indicator_File['metric_descriptor_code'] = EDWT_Indicator_File['metric_descriptor_code'].fillna(EDWT_Indicator_File['compare_code'].replace(compare_mapping))

# Drop the original columns
EDWT_Indicator_File.drop(columns=['improvement_code', 'compare_code'], inplace=True)

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceTrend', row['metric_descriptor_code']])
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceComparison', row['metric_descriptor_code']])

stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

# Add remaining columns
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

---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_1073/542316768.py in <cell line: 26>()
     24 
     25 # Round the non-NaN values
---> 26 EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)
     27 
     28 # Map IMPROVEMENT_IND_CODE to improvement_mapping

~/.local/lib/python3.10/site-packages/pandas/core/series.py in round(self, decimals, *args, **kwargs)
   2567         """
   2568         nv.validate_round(args, kwargs)
-> 2569         result = self._values.round(decimals)
   2570         result = self._constructor(result, index=self.index, copy=False).__finalize__(
   2571             self, method="round"

TypeError: can't multiply sequence by non-int of type 'float'
stacked_df






import pandas as pd

# Define mapping for PerformanceTrend values
performance_trend_mapping = {
    '1': 'Improving',
    '2': 'No Change',
    '3': 'Weakening',
}

# Define mapping for PerformanceComparison values
performance_comparison_mapping = {
    '1': 'Above average',
    '2': 'Same as average',
    '3': 'Below average',

}

# Create file for shallow slice pilot
# Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "improvement_code", "COMPARE_IND_CODE": "compare_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Map IMPROVEMENT_IND_CODE to improvement_mapping
EDWT_Indicator_File['metric_descriptor_group_code'] = EDWT_Indicator_File['improvement_code'].replace(performance_trend_mapping)

# Map COMPARE_IND_CODE to compare_mapping
EDWT_Indicator_File['metric_descriptor_group_code'] = EDWT_Indicator_File['metric_descriptor_group_code'].fillna(EDWT_Indicator_File['compare_code'].replace(performance_comparison_mapping))

# Map metric_descriptor_group_code to metric_descriptor_code
EDWT_Indicator_File['metric_descriptor_code'] = EDWT_Indicator_File['metric_descriptor_group_code'].map(lambda x: performance_trend_mapping[x] if x in performance_trend_mapping else performance_comparison_mapping[x])

# Drop the original columns
EDWT_Indicator_File.drop(columns=['improvement_code', 'compare_code'], inplace=True)

# Filter out rows with metric_descriptor_code as 999
EDWT_Indicator_File = EDWT_Indicator_File[EDWT_Indicator_File['metric_descriptor_code'] != '999']

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], row['metric_descriptor_group_code'], row['metric_descriptor_code']])

stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

# Add remaining columns
yr = "22" # Define yr variable
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

---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_1073/1408820059.py in <cell line: 27>()
     25 
     26 # Round the non-NaN values
---> 27 EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)
     28 
     29 # Map IMPROVEMENT_IND_CODE to improvement_mapping

~/.local/lib/python3.10/site-packages/pandas/core/series.py in round(self, decimals, *args, **kwargs)
   2567         """
   2568         nv.validate_round(args, kwargs)
-> 2569         result = self._values.round(decimals)
   2570         result = self._constructor(result, index=self.index, copy=False).__finalize__(
   2571             self, method="round"

TypeError: can't multiply sequence by non-int of type 'float'
