def apply_conditions(df):
    # Define the conditions and corresponding values
    conditions = [
        df['PERCENTILE_90'] < df['20th_Percentile'],
        (df['PERCENTILE_90'] >= df['20th_Percentile']) & (df['PERCENTILE_90'] <= df['80th_Percentile']),
        df['PERCENTILE_90'] > df['80th_Percentile']
    ]
    values = ['001', '002', '003']
    descriptions = ['Above average performance', 'Same as average', 'Below average performance']

    # Apply the conditions using `when` and `otherwise`
    df = df.withColumn('COMPARE_IND_CODE', when(conditions[0], values[0]).when(conditions[1], values[1]).otherwise(values[2]))
    df = df.withColumn('COMPARE_IND_E_DESC', when(conditions[0], descriptions[0]).when(conditions[1], descriptions[1]).otherwise(descriptions[2]))

    # Sort the DataFrame by 'CORP_ID' or 'REGION_ID' as needed 
    sort_column = 'CORP_ID' if 'CORP_ID' in df.columns else 'REGION_ID'
    df = df.orderBy(sort_column)

    return df

# Apply conditions to each DataFrame
los_org_cmp_a = apply_conditions(los_org_cmp)
tpia_org_cmp_a = apply_conditions(tpia_org_cmp)
los_reg_cmp_a = apply_conditions(los_reg_cmp)
tpia_reg_cmp_a = apply_conditions(tpia_reg_cmp)

# Assuming los_org_dfs, tpia_org_dfs, los_reg_dfs, tpia_reg_dfs are lists of DataFrames
# Perform successive merges and concatenate, merge, and rename operations

def successive_inner_merge(dataframes):
    result_df = dataframes[0]
    for df in dataframes[1:]:
        common_columns = list(set(result_df.columns) & set(df.columns))
        result_df = result_df.join(df, on=common_columns, how='inner')
    return result_df

# Perform successive merges for los_org and tpia_org
los_org_3x3 = successive_inner_merge(los_org_dfs)
tpia_org_3x3 = successive_inner_merge(tpia_org_dfs)

# Perform successive merges for los_reg and tpia_reg
los_reg_3x3 = successive_inner_merge(los_reg_dfs)
tpia_reg_3x3 = successive_inner_merge(tpia_reg_dfs)

# Concatenate, merge, and rename for Los Org and Tpia Org
los_org_all_yr_b = los_org_20.union(los_org_21).union(los_org_22_a).join(los_org_3x3, on='CORP_ID', how='inner').withColumnRenamed('FISCAL_YEAR', 'TIME')
tpia_org_all_yr_b = tpia_org_20.union(tpia_org_21).union(tpia_org_22_a).join(tpia_org_3x3, on='CORP_ID', how='inner').withColumnRenamed('FISCAL_YEAR', 'TIME')

# Concatenate, merge and rename for Los Reg and Tpia Reg
los_reg_all_yr_b = los_reg_20.union(los_reg_21).union(los_reg_22_a).join(los_reg_3x3, on='REGION_ID', how='inner').withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
tpia_reg_all_yr_b = tpia_reg_20.union(tpia_reg_21).union(tpia_reg_22_a).join(tpia_reg_3x3, on='REGION_ID', how='inner').withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')


---------------------------------------------------------------------------
NameError                                 Traceback (most recent call last)
/tmp/ipykernel_763/3275213181.py in <cell line: 22>()
     20 
     21 # Apply conditions to each DataFrame
---> 22 los_org_cmp_a = apply_conditions(los_org_cmp)
     23 tpia_org_cmp_a = apply_conditions(tpia_org_cmp)
     24 los_reg_cmp_a = apply_conditions(los_reg_cmp)

/tmp/ipykernel_763/3275213181.py in apply_conditions(df)
     10 
     11     # Apply the conditions using `when` and `otherwise`
---> 12     df = df.withColumn('COMPARE_IND_CODE', when(conditions[0], values[0]).when(conditions[1], values[1]).otherwise(values[2]))
     13     df = df.withColumn('COMPARE_IND_E_DESC', when(conditions[0], descriptions[0]).when(conditions[1], descriptions[1]).otherwise(descriptions[2]))
     14 

NameError: name 'when' is not defined
