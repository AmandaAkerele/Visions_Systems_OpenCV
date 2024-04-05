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
    TT_Spent_ED_File = TT_Spent_ED[["ORGANIZATION_ID", "FISCAL_YEAR_WH_ID", "improvement_descriptor_code", "compare_descriptor_code", "missing_reason_code"]]
    TT_Spent_ED_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "FISCAL_YEAR_WH_ID": "reporting_period_code"}, inplace=True)

    np.random.seed(0)
    scale_param = 30
    size = len(TT_Spent_ED_File)

    random_data = expon.ppf(np.random.rand(size), scale=scale_param)
    random_data_shifted = random_data + 1

    TT_Spent_ED_File['metric_result'] = random_data_shifted.round(1)
    TT_Spent_ED_File.dropna(subset=['metric_result'], inplace=True)

    stacked_data = []
    for index, row in TT_Spent_ED_File.iterrows():
        if row['missing_reason_code'] != '999':
            # For Row 1
            if row['missing_reason_code'] not in ['S03', 'S10', 'M02', 'S08']:
                stacked_data.append([row['reporting_entity_code'], row['metric_result'], '', '', row['missing_reason_code'], row['metric_result']])
            else:
                stacked_data.append([row['reporting_entity_code'], row['metric_result'], '', '', row['missing_reason_code'], ''])
            
            # For Row 2
            if row['improvement_descriptor_code'] != '999':
                stacked_data.append([row['reporting_entity_code'], '', 'PerformanceTrend', row['improvement_descriptor_code'], '', ''])
            
            # For Row 3
            if row['compare_descriptor_code'] != '999':
                stacked_data.append([row['reporting_entity_code'], '', 'PerformanceComparison', row['compare_descriptor_code'], '', ''])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', 'public_metric_result'])

    # Map reporting_period_code
    stacked_df['reporting_period_code'] = 'FY20' + stacked_df['reporting_period_code'].astype(str)
    
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
all_years_data.to_csv('fiscal_810_agg.csv', index=False)



correct 

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

KeyError: 'reporting_period_code'

The above exception was the direct cause of the following exception:

KeyError                                  Traceback (most recent call last)
/tmp/ipykernel_492/2300969136.py in <cell line: 96>()
     94 
     95 # Generate data for each year from FY2018 to FY2022
---> 96 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])
     97 
     98 # Write to CSV

/tmp/ipykernel_492/2300969136.py in <listcomp>(.0)
     94 
     95 # Generate data for each year from FY2018 to FY2022
---> 96 all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])
     97 
     98 # Write to CSV

/tmp/ipykernel_492/2300969136.py in generate_data_for_year(year)
     76 
     77     # Map reporting_period_code
---> 78     stacked_df['reporting_period_code'] = 'FY20' + stacked_df['reporting_period_code'].astype(str)
     79 
     80     stacked_df['reporting_entity_type_code'] = 'ORG'

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

KeyError: 'reporting_period_code'
