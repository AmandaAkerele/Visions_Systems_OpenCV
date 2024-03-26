from functools import reduce
from pyspark.sql import DataFrame

# Define a list of dataframes for los_org
los_org_dfs = [
    los_org_20.select('CORP_ID', 'CORP_PEER'),
    los_org_21.select('CORP_ID'),
    los_org_22_a.select('CORP_ID')
]

# Define a list of dataframes for tpia_org
tpia_org_dfs = [
    tpia_org_20.select('CORP_ID', 'CORP_PEER'),
    tpia_org_21.select('CORP_ID'),
    tpia_org_22_a.select('CORP_ID')
]

# Define a list of dataframes for los_reg
los_reg_dfs = [
    los_reg_20.select('REGION_ID'),
    los_reg_21.select('REGION_ID'),
    los_reg_22_a.select('REGION_ID')
]

# Define a list of dataframes for tpia_reg
tpia_reg_dfs = [
    tpia_reg_20.select('REGION_ID'),
    tpia_reg_21.select('REGION_ID'),
    tpia_reg_22_a.select('REGION_ID')
]

# Function to perform successive inner merges on a list of dataframes
def successive_inner_merge(df1, df2):
    return df1.join(df2, on=[col for col in df1.columns if col in df2.columns], how='inner')

# Perform the successive merges for los_org
los_org_3x3 = reduce(successive_inner_merge, los_org_dfs)

# Perform the successive merges for tpia_org
tpia_org_3x3 = reduce(successive_inner_merge, tpia_org_dfs)

# Perform the successive merges for los_reg
los_reg_3x3 = reduce(successive_inner_merge, los_reg_dfs)

# Perform the successive merges for tpia_reg
tpia_reg_3x3 = reduce(successive_inner_merge, tpia_reg_dfs)

# Check the counts after merging
print(f"los_org_3x3 count: {los_org_3x3.count()}")
print(f"tpia_org_3x3 count: {tpia_org_3x3.count()}")
print(f"los_reg_3x3 count: {los_reg_3x3.count()}")
print(f"tpia_reg_3x3 count: {tpia_reg_3x3.count()}")
s
