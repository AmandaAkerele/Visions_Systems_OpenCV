import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Patients Admitted Through the Emergency Department
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All[["ORGANIZATION_ID", "PCT_ADMT_ED"]]
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
nan_rows = IP_admit_ED_CORP_All_File[IP_admit_ED_CORP_All_File['metric_result'].isna() | IP_admit_ED_CORP_All_File['public_metric_result'].isna()]
nan_indices = nan_rows.index
IP_admit_ED_CORP_All_File.loc[nan_indices, 'missing_reason_code'] = 'M02'

# Ensure rows with NaN values are not filled with randomly generated numbers
IP_admit_ED_CORP_All_File.loc[nan_indices, ['metric_result', 'public_metric_result']] = np.nan

# Generate random percentages for metric_result and public_metric_result for non-NaN rows
non_nan_rows = IP_admit_ED_CORP_All_File.drop(nan_indices)
num_samples = len(non_nan_rows)
non_nan_rows['metric_result'] = np.random.uniform(0, 100, size=num_samples)
non_nan_rows['public_metric_result'] = non_nan_rows['metric_result']

# Combine NaN and non-NaN rows
IP_admit_ED_CORP_All_File = pd.concat([nan_rows, non_nan_rows]).sort_index()

# Reorder columns
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
IP_admit_ED_CORP_All_File.to_csv('815_agg_20' + yr + '.csv', index=False)









hhhh

import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Patients Admitted Through the Emergency Department
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All[["ORGANIZATION_ID", "PCT_ADMT_ED"]]
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
nan_rows = IP_admit_ED_CORP_All_File[IP_admit_ED_CORP_All_File['metric_result'].isna() | IP_admit_ED_CORP_All_File['public_metric_result'].isna()]
nan_indices = nan_rows.index
IP_admit_ED_CORP_All_File.loc[nan_indices, 'missing_reason_code'] = 'M02'

# Ensure rows with NaN values are not filled with randomly generated numbers
IP_admit_ED_CORP_All_File.loc[nan_indices, ['metric_result', 'public_metric_result']] = np.nan

# Generate random percentages for metric_result and public_metric_result for non-NaN rows
non_nan_rows = IP_admit_ED_CORP_All_File.dropna(subset=['metric_result', 'public_metric_result'])
num_samples = len(non_nan_rows)
non_nan_rows['metric_result'] = np.random.uniform(0, 100, size=num_samples)
non_nan_rows['public_metric_result'] = non_nan_rows['metric_result']

# Reorder columns
IP_admit_ED_CORP_All_File = pd.concat([nan_rows, non_nan_rows]).sort_index()

IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
IP_admit_ED_CORP_All_File.to_csv('815_agg_20' + yr + '.csv', index=False)



fffffg





Here's the modified code for generating random percentage values for the given scenario:

```python
import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Patient Days in Alternate Level of Care (Percentage)

# Dataframes with columns renamed
ALC_LOS_CORP_1 = ALC_LOS_CORP[["ORGANIZATION_ID", "PCT_ALC_LOS"]]
ALC_LOS_CORP_1.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "PCT_ALC_LOS": "metric_result"}, inplace=True)

ALC_REG_1 = ALC_REG[["REGIONAL_ORGANIZATION_ID", "PCT_ALC_LOS"]]
ALC_REG_1.rename(columns={"REGIONAL_ORGANIZATION_ID": "reporting_entity_code", "PCT_ALC_LOS": "metric_result"}, inplace=True)

ALC_PROV_1 = ALC_PROV[["PROVINCIAL_ORGANIZATION_ID", "PCT_ALC_LOS"]]
ALC_PROV_1.rename(columns={"PROVINCIAL_ORGANIZATION_ID": "reporting_entity_code", "PCT_ALC_LOS": "metric_result"}, inplace=True)

# Concatenate dataframes
ALC_File = pd.concat([ALC_LOS_CORP_1, ALC_REG_1, ALC_PROV_1], ignore_index=True)

# Additional columns added
ALC_File['reporting_period_code'] = 'FY20' + yr
ALC_File['reporting_entity_type_code'] = 'ORG'
ALC_File['indicator_code'] = '816'
ALC_File['metric_code'] = 'PERCENT'
ALC_File['breakdown_type_code_l1'] = 'N/A'
ALC_File['breakdown_value_code_l1'] = 'N/A'
ALC_File['breakdown_type_code_l2'] = 'N/A'
ALC_File['breakdown_value_code_l2'] = 'N/A'
ALC_File['metric_descriptor_group_code'] = ''
ALC_File['metric_descriptor_code'] = ''

# Generate random percentage values for metric_result and public_metric_result
num_samples = len(ALC_File)
# Generating random percentage values between 0 and 100
ALC_File['metric_result'] = np.random.uniform(0, 100, size=num_samples)
ALC_File['public_metric_result'] = ALC_File['metric_result']

# Fill NaN values with "M02" in missing_reason_code column
ALC_File['missing_reason_code'] = ALC_File['metric_result'].isna().astype(int).map({1: 'M02', 0: ''})

# Reorder columns
ALC_File = ALC_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
ALC_File.to_csv('816_agg_20' + yr + '.csv', index=False)
```

This code will generate random floating-point numbers between 0 and 100, representing percentages, for both `metric_result` and `public_metric_result` columns. Make sure to import `numpy` as `np` at the beginning of your script.




import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Number of Emergency Department Visits

# Dataframes with columns renamed
ED_CORP_File = ED_CORP[["CORP_ID", "ED_CORP_CNT"]]
ED_CORP_File.rename(columns={"CORP_ID": "reporting_entity_code", "ED_CORP_CNT": "metric_result"}, inplace=True)

# Additional columns added
ED_CORP_File['reporting_period_code'] = 'FY20' + yr
ED_CORP_File['reporting_entity_type_code'] = 'ORG'
ED_CORP_File['indicator_code'] = '814'
ED_CORP_File['metric_code'] = 'NUMBER_OF_CASES'
ED_CORP_File['breakdown_type_code_l1'] = 'N/A'
ED_CORP_File['breakdown_value_code_l1'] = 'N/A'
ED_CORP_File['breakdown_type_code_l2'] = 'N/A'
ED_CORP_File['breakdown_value_code_l2'] = 'N/A'
ED_CORP_File['metric_descriptor_group_code'] = ''
ED_CORP_File['metric_descriptor_code'] = ''

# Generate random number of cases for metric_result and public_metric_result
num_samples = len(ED_CORP_File)
# Generating random integer values between 0 and 1000 (adjust range as needed)
ED_CORP_File['metric_result'] = np.random.randint(0, 1001, size=num_samples)
ED_CORP_File['public_metric_result'] = ED_CORP_File['metric_result']

# Fill NaN values with "M02" in missing_reason_code column
ED_CORP_File['missing_reason_code'] = ED_CORP_File['metric_result'].isna().astype(int).map({1: 'M02', 0: ''})

# Reorder columns
ED_CORP_File = ED_CORP_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
ED_CORP_File.to_csv('814_agg_20' + yr + '.csv', index=False)


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

# Generate random percentages for metric_result and public_metric_result
num_samples = len(IP_admit_ED_CORP_All_File)
# Generating random percentage values between 0 and 100
IP_admit_ED_CORP_All_File['metric_result'] = np.random.uniform(0, 100, size=num_samples)
IP_admit_ED_CORP_All_File['public_metric_result'] = IP_admit_ED_CORP_All_File['metric_result']

# Fill NaN values with "M02" in missing_reason_code column
IP_admit_ED_CORP_All_File['missing_reason_code'] = IP_admit_ED_CORP_All_File['metric_result'].isna().astype(int).map({1: 'M02', 0: ''})

# Reorder columns
IP_admit_ED_CORP_All_File = IP_admit_ED_CORP_All_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
IP_admit_ED_CORP_All_File.to_csv('815_agg_20' + yr + '.csv', index=False)




import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Total Acute Care Resource Use Intensity

# Dataframes with columns renamed
IP_RIW_CORP_TOTAL_File = IP_RIW_CORP_All[["ORGANIZATION_ID",  "RIW_SUM"]]
IP_RIW_CORP_TOTAL_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "RIW_SUM": "metric_result"}, inplace=True)

# Additional columns added
IP_RIW_CORP_TOTAL_File['reporting_period_code'] = 'FY20' + yr
IP_RIW_CORP_TOTAL_File['reporting_entity_type_code'] = 'ORG'
IP_RIW_CORP_TOTAL_File['indicator_code'] = '817'
IP_RIW_CORP_TOTAL_File['metric_code'] = 'TOTAL'
IP_RIW_CORP_TOTAL_File['breakdown_type_code_l1'] = 'N/A'
IP_RIW_CORP_TOTAL_File['breakdown_value_code_l1'] = 'N/A'
IP_RIW_CORP_TOTAL_File['breakdown_type_code_l2'] = 'N/A'
IP_RIW_CORP_TOTAL_File['breakdown_value_code_l2'] = 'N/A'
IP_RIW_CORP_TOTAL_File['metric_descriptor_group_code'] = ''
IP_RIW_CORP_TOTAL_File['metric_descriptor_code'] = ''

# Generate random values for metric_result and public_metric_result
num_samples = len(IP_RIW_CORP_TOTAL_File)
# Generating random floating-point numbers between 0 and 1000 (adjust range as needed)
IP_RIW_CORP_TOTAL_File['metric_result'] = np.random.uniform(0, 1000, size=num_samples)
IP_RIW_CORP_TOTAL_File['public_metric_result'] = IP_RIW_CORP_TOTAL_File['metric_result']

# Fill NaN values with "M02" in missing_reason_code column
IP_RIW_CORP_TOTAL_File['missing_reason_code'] = IP_RIW_CORP_TOTAL_File['metric_result'].isna().astype(int).map({1: 'M02', 0: ''})

# Reorder columns
IP_RIW_CORP_TOTAL_File = IP_RIW_CORP_TOTAL_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
IP_RIW_CORP_TOTAL_File.to_csv('817_agg_20' + yr + '.csv', index=False)





import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Average Acute Care Resource Use Intensity

# Dataframes with columns renamed
IP_RIW_CORP_AVG_File = IP_RIW_CORP_All[["ORGANIZATION_ID",  "RIW_AVG"]]
IP_RIW_CORP_AVG_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "RIW_AVG": "metric_result"}, inplace=True)

# Additional columns added
IP_RIW_CORP_AVG_File['reporting_period_code'] = 'FY20' + yr
IP_RIW_CORP_AVG_File['reporting_entity_type_code'] = 'ORG'
IP_RIW_CORP_AVG_File['indicator_code'] = '818'
IP_RIW_CORP_AVG_File['metric_code'] = 'AVERAGE'
IP_RIW_CORP_AVG_File['breakdown_type_code_l1'] = 'N/A'
IP_RIW_CORP_AVG_File['breakdown_value_code_l1'] = 'N/A'
IP_RIW_CORP_AVG_File['breakdown_type_code_l2'] = 'N/A'
IP_RIW_CORP_AVG_File['breakdown_value_code_l2'] = 'N/A'
IP_RIW_CORP_AVG_File['metric_descriptor_group_code'] = ''
IP_RIW_CORP_AVG_File['metric_descriptor_code'] = ''

# Generate random values for metric_result and public_metric_result
num_samples = len(IP_RIW_CORP_AVG_File)
# Generating random floating-point numbers between 0 and 1000 (adjust range as needed)
IP_RIW_CORP_AVG_File['metric_result'] = np.random.uniform(0, 1000, size=num_samples)
IP_RIW_CORP_AVG_File['public_metric_result'] = IP_RIW_CORP_AVG_File['metric_result']

# Fill NaN values with "M02" in missing_reason_code column
IP_RIW_CORP_AVG_File['missing_reason_code'] = IP_RIW_CORP_AVG_File['metric_result'].isna().astype(int).map({1: 'M02', 0: ''})

# Reorder columns
IP_RIW_CORP_AVG_File = IP_RIW_CORP_AVG_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2',



To generate random values for the Total Number of Acute Care Hospital Stays, you can use the `numpy` library to generate random integers representing the total number of stays. Here's the modified code:

```python
import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Number of Acute Care Hospital Stays

# Dataframes with columns renamed
IP_LOS_CORP_TOTAL_File = ip_los_corp_All[["ORGANIZATION_ID",  "SUM_TOTAL_LOS"]]
IP_LOS_CORP_TOTAL_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "SUM_TOTAL_LOS": "metric_result"}, inplace=True)

# Additional columns added
IP_LOS_CORP_TOTAL_File['reporting_period_code'] = 'FY20' + yr
IP_LOS_CORP_TOTAL_File['reporting_entity_type_code'] = 'ORG'
IP_LOS_CORP_TOTAL_File['indicator_code'] = '812'
IP_LOS_CORP_TOTAL_File['metric_code'] = 'TOTAL'
IP_LOS_CORP_TOTAL_File['breakdown_type_code_l1'] = 'N/A'
IP_LOS_CORP_TOTAL_File['breakdown_value_code_l1'] = 'N/A'
IP_LOS_CORP_TOTAL_File['breakdown_type_code_l2'] = 'N/A'
IP_LOS_CORP_TOTAL_File['breakdown_value_code_l2'] = 'N/A'
IP_LOS_CORP_TOTAL_File['metric_descriptor_group_code'] = ''
IP_LOS_CORP_TOTAL_File['metric_descriptor_code'] = ''

# Generate random values for metric_result and public_metric_result
num_samples = len(IP_LOS_CORP_TOTAL_File)
# Generating random integer values between 0 and 1000 (adjust range as needed)
IP_LOS_CORP_TOTAL_File['metric_result'] = np.random.randint(0, 1001, size=num_samples)
IP_LOS_CORP_TOTAL_File['public_metric_result'] = IP_LOS_CORP_TOTAL_File['metric_result']

# Fill NaN values with "M02" in missing_reason_code column
IP_LOS_CORP_TOTAL_File['missing_reason_code'] = IP_LOS_CORP_TOTAL_File['metric_result'].isna().astype(int).map({1: 'M02', 0: ''})

# Reorder columns
IP_LOS_CORP_TOTAL_File = IP_LOS_CORP_TOTAL_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
IP_LOS_CORP_TOTAL_File.to_csv('812_agg_20' + yr + '.csv', index=False)
```


import pandas as pd
import numpy as np

# Create file for shallow slice pilot 
# Contextual Measure: Average Length of a Hospital Stay (Days)

# Dataframes with columns renamed
IP_LOS_CORP_AVG_File = ip_los_corp_All[["ORGANIZATION_ID",  "AVG_LOS"]]
IP_LOS_CORP_AVG_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code", "AVG_LOS": "metric_result"}, inplace=True)

# Additional columns added
IP_LOS_CORP_AVG_File['reporting_period_code'] = 'FY20' + yr
IP_LOS_CORP_AVG_File['reporting_entity_type_code'] = 'ORG'
IP_LOS_CORP_AVG_File['indicator_code'] = '813'
IP_LOS_CORP_AVG_File['metric_code'] = 'AVERAGE'
IP_LOS_CORP_AVG_File['breakdown_type_code_l1'] = 'N/A'
IP_LOS_CORP_AVG_File['breakdown_value_code_l1'] = 'N/A'
IP_LOS_CORP_AVG_File['breakdown_type_code_l2'] = 'N/A'
IP_LOS_CORP_AVG_File['breakdown_value_code_l2'] = 'N/A'
IP_LOS_CORP_AVG_File['metric_descriptor_group_code'] = ''
IP_LOS_CORP_AVG_File['metric_descriptor_code'] = ''

# Generate random values for metric_result and public_metric_result
num_samples = len(IP_LOS_CORP_AVG_File)
# Generating random floating-point numbers between 0 and 30 (representing days, adjust range as needed)
IP_LOS_CORP_AVG_File['metric_result'] = np.random.uniform(0, 30, size=num_samples)
IP_LOS_CORP_AVG_File['public_metric_result'] = IP_LOS_CORP_AVG_File['metric_result']

# Fill NaN values with "M02" in missing_reason_code column
IP_LOS_CORP_AVG_File['missing_reason_code'] = IP_LOS_CORP_AVG_File['metric_result'].isna().astype(int).map({1: 'M02', 0: ''})

# Reorder columns
IP_LOS_CORP_AVG_File = IP_LOS_CORP_AVG_File[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                   'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                   'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

# Write to CSV
IP_LOS_CORP_AVG_File.to_csv('813_agg_20' + yr + '.csv', index=False)


xxxxxxxxxx


















