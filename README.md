from functools import reduce
from pyspark.sql import DataFrame

# Function to perform successive inner joins on a list of DataFrames
def successive_inner_join(dataframes):
    # Define a function to perform inner join between two DataFrames
    def inner_join(df1, df2):
        return df1.join(df2, on=list(set(df1.columns) & set(df2.columns)), how='inner')
    
    # Use reduce to apply inner join function successively
    return reduce(inner_join, dataframes)

# Perform the successive joins for los_org
los_org_3x3 = successive_inner_join(los_org_dfs)

# Perform the successive joins for tpia_org
tpia_org_3x3 = successive_inner_join(tpia_org_dfs)

# Perform the successive joins for los_reg
los_reg_3x3 = successive_inner_join(los_reg_dfs)

# Perform the successive joins for tpia_reg
tpia_reg_3x3 = successive_inner_join(tpia_reg_dfs)
