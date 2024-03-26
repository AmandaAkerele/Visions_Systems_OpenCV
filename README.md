from functools import reduce
from pyspark.sql import DataFrame

# Function to perform successive inner merges on a list of dataframes
def successive_inner_merge(dataframes):
    result_df = dataframes[0]
    for df in dataframes[1:]:
        common_columns = list(set(result_df.columns) & set(df.columns))
        result_df = result_df.join(df, on=common_columns, how='inner')
    return result_df

# Perform the successive merges for los_org
los_org_3x3 = successive_inner_merge([
    los_org_20.select('CORP_ID', 'CORP_PEER'),
    los_org_21.select('CORP_ID'),
    los_org_22_a.select('CORP_ID')
])

# Perform the successive merges for tpia_org
tpia_org_3x3 = successive_inner_merge([
    tpia_org_20.select('CORP_ID', 'CORP_PEER'),
    tpia_org_21.select('CORP_ID'),
    tpia_org_22_a.select('CORP_ID')
])

# Perform the successive merges for los_reg
los_reg_3x3 = successive_inner_merge([
    los_reg_20.select('REGION_ID'),
    los_reg_21.select('REGION_ID'),
    los_reg_22_a.select('REGION_ID')
])

# Perform the successive merges for tpia_reg
tpia_reg_3x3 = successive_inner_merge([
    tpia_reg_20.select('REGION_ID'),
    tpia_reg_21.select('REGION_ID'),
    tpia_reg_22_a.select('REGION_ID')
])
