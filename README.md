from functools import reduce
from pyspark.sql import DataFrame

# Function to perform successive inner merges on a list of dataframes
def successive_inner_merge(dataframes):
    return reduce(DataFrame.join, dataframes)

# Selecting necessary columns from each DataFrame
los_org_20_selected = los_org_20.select('CORP_ID', 'CORP_PEER')
los_org_21_selected = los_org_21.select('CORP_ID')
los_org_22_a_selected = los_org_22_a.select('CORP_ID')

tpia_org_20_selected = tpia_org_20.select('CORP_ID', 'CORP_PEER')
tpia_org_21_selected = tpia_org_21.select('CORP_ID')
tpia_org_22_a_selected = tpia_org_22_a.select('CORP_ID')

los_reg_20_selected = los_reg_20.select('REGION_ID')
los_reg_21_selected = los_reg_21.select('REGION_ID')
los_reg_22_a_selected = los_reg_22_a.select('REGION_ID')

tpia_reg_20_selected = tpia_reg_20.select('REGION_ID')
tpia_reg_21_selected = tpia_reg_21.select('REGION_ID')
tpia_reg_22_a_selected = tpia_reg_22_a.select('REGION_ID')

# Perform the successive merges
los_org_3x3 = successive_inner_merge([
    los_org_20_selected,
    los_org_21_selected,
    los_org_22_a_selected
])

tpia_org_3x3 = successive_inner_merge([
    tpia_org_20_selected,
    tpia_org_21_selected,
    tpia_org_22_a_selected
])

los_reg_3x3 = successive_inner_merge([
    los_reg_20_selected,
    los_reg_21_selected,
    los_reg_22_a_selected
])

tpia_reg_3x3 = successive_inner_merge([
    tpia_reg_20_selected,
    tpia_reg_21_selected,
    tpia_reg_22_a_selected
])



or 




from pyspark.sql import DataFrame

# Function to perform successive inner merges on a list of dataframes using SQL
def successive_inner_merge_sql(dataframes):
    # Registering each DataFrame as a temporary table
    for idx, df in enumerate(dataframes):
        df.createOrReplaceTempView(f"temp_df_{idx}")
    
    # Constructing the SQL join query
    sql_query = f"SELECT * FROM temp_df_0"
    for idx in range(1, len(dataframes)):
        common_columns = list(set(dataframes[idx-1].columns) & set(dataframes[idx].columns))
        on_clause = " AND ".join([f"temp_df_{idx-1}.{col} = temp_df_{idx}.{col}" for col in common_columns])
        sql_query += f" JOIN temp_df_{idx} ON {on_clause}"
    
    # Executing the SQL query
    result_df = spark.sql(sql_query)
    
    return result_df

# Selecting necessary columns from each DataFrame
los_org_20_selected = los_org_20.select('CORP_ID', 'CORP_PEER')
los_org_21_selected = los_org_21.select('CORP_ID')
los_org_22_a_selected = los_org_22_a.select('CORP_ID')

tpia_org_20_selected = tpia_org_20.select('CORP_ID', 'CORP_PEER')
tpia_org_21_selected = tpia_org_21.select('CORP_ID')
tpia_org_22_a_selected = tpia_org_22_a.select('CORP_ID')

los_reg_20_selected = los_reg_20.select('REGION_ID')
los_reg_21_selected = los_reg_21.select('REGION_ID')
los_reg_22_a_selected = los_reg_22_a.select('REGION_ID')

tpia_reg_20_selected = tpia_reg_20.select('REGION_ID')
tpia_reg_21_selected = tpia_reg_21.select('REGION_ID')
tpia_reg_22_a_selected = tpia_reg_22_a.select('REGION_ID')

# Perform the successive merges using SQL
los_org_3x3 = successive_inner_merge_sql([
    los_org_20_selected,
    los_org_21_selected,
    los_org_22_a_selected
])

tpia_org_3x3 = successive_inner_merge_sql([
    tpia_org_20_selected,
    tpia_org_21_selected,
    tpia_org_22_a_selected
])

los_reg_3x3 = successive_inner_merge_sql([
    los_reg_20_selected,
    los_reg_21_selected,
    los_reg_22_a_selected
])

tpia_reg_3x3 = successive_inner_merge_sql([
    tpia_reg_20_selected,
    tpia_reg_21_selected,
    tpia_reg_22_a_selected
])
