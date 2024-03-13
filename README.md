import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Patients Admitted Through the Emergency Department
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All[["ORGANIZATION_ID",  "PCT_ADMT_ED"]]
IP_admit_ED_CORP_All_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "PCT_ADMT_ED": "metric_result"}, inplace=True)

# Additional columns added
IP_admit_ED_CORP_All_File['reporting_period_code'] = 'FY20' + yr
IP_admit_ED_CORP_All_File['reporting_entity_type_code'] = 'ORG'
IP_admit_ED_CORP_All_File['indicator_code'] = '815'
IP_admit_ED_CORP_All_File['metric_code'] = 'PERCENT'
IP_admit_ED_CORP_All_File['breakdown_type_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_type_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['metric_descriptor_group_code'] = ''
IP_admit_ED_CORP_All_File['metric_descriptor_code'] = ''

# Fill NaN values with "M02" in missing_reason_code column and highlight rows with NaN values
IP_admit_ED_CORP_All_File['missing_reason_code'] = ''
nan_indices = IP_admit_ED_CORP_All_File[IP_admit_ED_CORP_All_File['metric_result'].isna()].index
IP_admit_ED_CORP_All_File.loc[nan_indices, 'missing_reason_code'] = 'M02'

# Generate random percentages for metric_result for non-NaN rows
non_nan_indices = IP_admit_ED_CORP_All_File[IP_admit_ED_CORP_All_File['missing_reason_code'] != 'M02'].index
num_non_nan = len(non_nan_indices)
IP_admit_ED_CORP_All_File.loc[non_nan_indices, 'metric_result'] = np.random.uniform(0, 100, size=num_non_nan)

# Ensure public_metric_result column is present
IP_admit_ED_CORP_All_File['public_metric_result'] = IP_admit_ED_CORP_All_File['metric_result']

# Ensure missing_reason_code column is present
if 'missing_reason_code' not in IP_admit_ED_CORP_All_File.columns:
    IP_admit_ED_CORP_All_File['missing_reason_code'] = ''

# Reorder columns to ensure all columns are present
all_columns = ['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code',
               'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1',
               'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result',
               'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code',
               'public_metric_result']

IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All_File.reindex(columns=all_columns)

# Write to CSV
IP_admit_ED_CORP_All_File.to_csv('815_agg_20' + yr + '.csv', index=False)




nowwwe
 
 
 
 
 pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Patients Admitted Through the Emergency Department
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All[["ORGANIZATION_ID",  "PCT_ADMT_ED"]]
IP_admit_ED_CORP_All_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "PCT_ADMT_ED": "metric_result"}, inplace=True)

# Additional columns added
IP_admit_ED_CORP_All_File['reporting_period_code'] = 'FY20' + yr
IP_admit_ED_CORP_All_File['reporting_entity_type_code'] = 'ORG'
IP_admit_ED_CORP_All_File['indicator_code'] = '815'
IP_admit_ED_CORP_All_File['metric_code'] = 'PERCENT'
IP_admit_ED_CORP_All_File['breakdown_type_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_type_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['metric_descriptor_group_code'] = ''
IP_admit_ED_CORP_All_File['metric_descriptor_code'] = ''

# Fill NaN values with "M02" in missing_reason_code column and highlight rows with NaN values
IP_admit_ED_CORP_All_File['missing_reason_code'] = ''
nan_rows = IP_admit_ED_CORP_All_File[IP_admit_ED_CORP_All_File['metric_result'].isna()]
nan_indices = nan_rows.index
IP_admit_ED_CORP_All_File.loc[nan_indices, 'missing_reason_code'] = 'M02'

# Ensure rows with NaN values are not filled with randomly generated numbers
IP_admit_ED_CORP_All_File.loc[nan_indices, 'metric_result'] = np.nan

# Generate random percentages for metric_result for non-NaN rows
non_nan_rows = IP_admit_ED_CORP_All_File.drop(nan_indices)
num_samples = len(non_nan_rows)
non_nan_rows['metric_result'] = np.random.uniform(0, 100, size=num_samples)

# Combine NaN and non-NaN rows
IP_admit_ED_CORP_All_File = pd.concat([nan_rows, non_nan_rows]).sort_index()

# Ensure public_metric_result column is present
IP_admit_ED_CORP_All_File['public_metric_result'] = IP_admit_ED_CORP_All_File['metric_result']

# Reorder columns to ensure all columns are present
all_columns = ['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code',
               'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1',
               'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result',
               'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code',
               'public_metric_result']

IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All_File.reindex(columns=all_columns)

# Write to CSV
IP_admit_ED_CORP_All_File.to_csv('815_agg_20' + yr + '.csv', index=False)




gggggg




import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Patients Admitted Through the Emergency Department
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All[["ORGANIZATION_ID",  "PCT_ADMT_ED"]]
IP_admit_ED_CORP_All_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "PCT_ADMT_ED": "metric_result"}, inplace=True)

# Additional columns added
IP_admit_ED_CORP_All_File['reporting_period_code'] = 'FY20' + yr
IP_admit_ED_CORP_All_File['reporting_entity_type_code'] = 'ORG'
IP_admit_ED_CORP_All_File['indicator_code'] = '815'
IP_admit_ED_CORP_All_File['metric_code'] = 'PERCENT'
IP_admit_ED_CORP_All_File['breakdown_type_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_type_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['metric_descriptor_group_code'] = ''
IP_admit_ED_CORP_All_File['metric_descriptor_code'] = ''

# Fill NaN values with "M02" in missing_reason_code column and highlight rows with NaN values
IP_admit_ED_CORP_All_File['missing_reason_code'] = ''
nan_rows = IP_admit_ED_CORP_All_File[IP_admit_ED_CORP_All_File['metric_result'].isna()]
nan_indices = nan_rows.index
IP_admit_ED_CORP_All_File.loc[nan_indices, 'missing_reason_code'] = 'M02'

# Ensure rows with NaN values are not filled with randomly generated numbers
IP_admit_ED_CORP_All_File.loc[nan_indices, 'metric_result'] = np.nan

# Generate random percentages for metric_result for non-NaN rows
non_nan_rows = IP_admit_ED_CORP_All_File.drop(nan_indices)
num_samples = len(non_nan_rows)
non_nan_rows['metric_result'] = np.random.uniform(0, 100, size=num_samples)

# Combine NaN and non-NaN rows
IP_admit_ED_CORP_All_File = pd.concat([nan_rows, non_nan_rows]).sort_index()

# Reorder columns to ensure all columns are present
all_columns = ['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code',
               'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1',
               'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result',
               'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code']

IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All_File.reindex(columns=all_columns)

# Write to CSV
IP_admit_ED_CORP_All_File.to_csv('815_agg_20' + yr + '.csv', index=False)







or 

import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Patients Admitted Through the Emergency Department
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All[["ORGANIZATION_ID",  "PCT_ADMT_ED"]]
IP_admit_ED_CORP_All_File.rename(columns={"ORGANIZATION_ID":"reporting_entity_code", "PCT_ADMT_ED": "metric_result"}, inplace=True)

# Additional columns added
IP_admit_ED_CORP_All_File['reporting_period_code'] = 'FY20' + yr
IP_admit_ED_CORP_All_File['reporting_entity_type_code'] = 'ORG'
IP_admit_ED_CORP_All_File['indicator_code'] = '815'
IP_admit_ED_CORP_All_File['metric_code'] = 'PERCENT'
IP_admit_ED_CORP_All_File['breakdown_type_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l1'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_type_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['breakdown_value_code_l2'] = 'N/A'
IP_admit_ED_CORP_All_File['metric_descriptor_group_code'] = ''
IP_admit_ED_CORP_All_File['metric_descriptor_code'] = ''

# Fill NaN values with "M02" in missing_reason_code column and highlight rows with NaN values
IP_admit_ED_CORP_All_File['missing_reason_code'] = ''
nan_rows = IP_admit_ED_CORP_All_File[IP_admit_ED_CORP_All_File['metric_result'].isna()]
nan_indices = nan_rows.index
IP_admit_ED_CORP_All_File.loc[nan_indices, 'missing_reason_code'] = 'M02'

# Ensure rows with NaN values are not filled with randomly generated numbers
IP_admit_ED_CORP_All_File.loc[nan_indices, 'metric_result'] = np.nan

# Generate random percentages for metric_result for non-NaN rows
non_nan_rows = IP_admit_ED_CORP_All_File.drop(nan_indices)
num_samples = len(non_nan_rows)
non_nan_rows['metric_result'] = np.random.uniform(0, 100, size=num_samples)

# Combine NaN and non-NaN rows
IP_admit_ED_CORP_All_File = pd.concat([nan_rows, non_nan_rows]).sort_index()

# Reorder columns
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code']]

# Write to CSV
IP_admit_ED_CORP_All_File.to_csv('815_agg_20' + yr + '.csv', index=False)

