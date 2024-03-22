if metric_descriptor_code = 999 where metric_descriptor_group_code = PerformanceTrend then drop that row



# Drop rows where metric_descriptor_code is 999 and metric_descriptor_group_code is PerformanceTrend
EDWT_Indicator_File = EDWT_Indicator_File[~((EDWT_Indicator_File['metric_descriptor_code'] == '999') & (EDWT_Indicator_File['metric_descriptor_group_code'] == 'PerformanceTrend'))]





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

# Function to map metric_descriptor_code based on metric_descriptor_group_code
def map_metric_descriptor(row):
    if row['metric_descriptor_group_code'] == 'PerformanceTrend':
        return {
            '1': 'Improving',
            '2': 'No Change',
            '3': 'Weakening'
        }.get(row['metric_descriptor_code'], '')
    elif row['metric_descriptor_group_code'] == 'PerformanceComparison':
        return {
            '1': 'Above average',
            '2': 'Same as average',
            '3': 'Below average'
        }.get(row['metric_descriptor_code'], '')
    else:
        return ''

# Apply mapping function
EDWT_Indicator_File['metric_descriptor_code'] = EDWT_Indicator_File.apply(map_metric_descriptor, axis=1)

# Drop rows with metric_descriptor_code as '999'
EDWT_Indicator_File = EDWT_Indicator_File[EDWT_Indicator_File['metric_descriptor_code'] != '999']

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], row['metric_descriptor_group_code'], row['metric_descriptor_code']])

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


kkkkkkkkkkkkk







map so that when metric_descriptor_group_code 

I have a column called metric_descriptor_group_code  and this column has two result populated as rows which are PerformanceTrend or PerformanceComparison.

I have another column that is named metric_descriptor_code which has values of 1, 2, 3 or 999. 

I want these values in the column named metric_descriptor_code to read as follows 

when row is metric_descriptor_code  = '1' then metric_descriptor_code should be 'Improving', when the metric_description_group_code result is PerformanceTrend 


when row is metric_descriptor_code  = '2' then metric_descriptor_code should be 'No Change', when the metric_description_group_code result is  PerformanceTrend 

when row is metric_descriptor_code  = '3' then metric_descriptor_code should be 'Weakening', when the metric_description_group_code result is PerformanceTrend 

when row is metric_descriptor_code  = '1' then metric_descriptor_code should be 'Above average', when the metric_description_group_code result is PerformanceComparison

when row is metric_descriptor_code  = '2' then metric_descriptor_code should be 'Same as average', when the metric_description_group_code result is PerformanceComparison

when row is metric_descriptor_code  = '3' then metric_descriptor_code should be 'Below average', when the metric_description_group_code result is PerformanceComparison

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
EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID",  "INDICATOR_VALUE", "IMPROVEMENT_IND_CODE", "COMPARE_IND_CODE", "PerformanceTrend", "PerformanceComparison"]]
EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "INDICATOR_VALUE": "metric_result", "IMPROVEMENT_IND_CODE": "improvement_code", "COMPARE_IND_CODE": "compare_code"}, inplace=True)

# Drop rows with NaN values in the 'metric_result' column
EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

# Round the non-NaN values
EDWT_Indicator_File['metric_result'] = EDWT_Indicator_File['metric_result'].round(1)

# Apply the condition for PerformanceTrend
EDWT_Indicator_File.loc[EDWT_Indicator_File['metric_descriptor_group_code'] == 'PerformanceTrend', 'metric_descriptor_code'] = EDWT_Indicator_File['PerformanceTrend'].map({
    '1': 'Improving',
    '2': 'No Change',
    '3': 'Weakening'
})

# Apply the condition for PerformanceComparison
EDWT_Indicator_File.loc[EDWT_Indicator_File['metric_descriptor_group_code'] == 'PerformanceComparison', 'metric_descriptor_code'] = EDWT_Indicator_File['PerformanceComparison'].map({
    '1': 'Above average',
    '2': 'Same as average',
    '3': 'Below average'
})

# Drop the original columns
EDWT_Indicator_File.drop(columns=['improvement_code', 'compare_code', 'PerformanceTrend', 'PerformanceComparison'], inplace=True)

# Stack the rows
stacked_data = []
for index, row in EDWT_Indicator_File.iterrows():
    stacked_data.append([row['reporting_entity_code'], row['metric_result'], row['metric_descriptor_group_code'], row['metric_descriptor_code']])

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
