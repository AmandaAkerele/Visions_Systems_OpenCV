# Rename columns for los_org_cmp
los_org_cmp_a = los_org_cmp.withColumnRenamed("0.2", "20th_Percentile").withColumnRenamed("0.8", "80th_Percentile")

# Rename columns for tpia_org_cmp
tpia_org_cmp_a = tpia_org_cmp.withColumnRenamed("0.2", "20th_Percentile").withColumnRenamed("0.8", "80th_Percentile")

# Rename columns for los_reg_cmp
los_reg_cmp_a = los_reg_cmp.withColumnRenamed("0.2", "20th_Percentile").withColumnRenamed("0.8", "80th_Percentile")

# Rename columns for tpia_reg_cmp
tpia_reg_cmp_a = tpia_reg_cmp.withColumnRenamed("0.2", "20th_Percentile").withColumnRenamed("0.8", "80th_Percentile")


from pyspark.sql import functions as F

def apply_conditions(df):
    conditions = [
        (F.col('PERCENTILE_90') < F.col('20th_Percentile')),
        (F.col('PERCENTILE_90') >= F.col('20th_Percentile')) & (F.col('PERCENTILE_90') <= F.col('80th_Percentile')),
        (F.col('PERCENTILE_90') > F.col('80th_Percentile'))
    ]

    values = ['001', '002', '003']
    descriptions = ['Above average performance', 'Same as average', 'Below average performance']

    # Apply conditions
    df = df.withColumn('COMPARE_IND_CODE', F.when(conditions[0], values[0])
                                           .when(conditions[1], values[1])
                                           .when(conditions[2], values[2])
                                           .otherwise(''))

    df = df.withColumn('COMPARE_IND_E_DESC', F.when(conditions[0], descriptions[0])
                                               .when(conditions[1], descriptions[1])
                                               .when(conditions[2], descriptions[2])
                                               .otherwise(''))

    # Sort the DataFrame by CORP_ID or REGION_ID as needed 
    if 'CORP_ID' in df.columns:
        df = df.orderBy('CORP_ID')
    elif 'REGION_ID' in df.columns:
        df = df.orderBy('REGION_ID')

    return df

# Apply conditions for each DataFrame
los_org_cmp_a = apply_conditions(los_org_cmp_a)
tpia_org_cmp_a = apply_conditions(tpia_org_cmp_a)
los_reg_cmp_a = apply_conditions(los_reg_cmp_a)
tpia_reg_cmp_a = apply_conditions(tpia_reg_cmp_a)







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
def successive_inner_merge(dataframes):
    result_df = dataframes[0]
    for df in dataframes[1:]:
        result_df = result_df.join(df, on=list(set(result_df.columns) & set(df.columns)), how='inner')
    return result_df

# Perform the successive merges for los_org
los_org_3x3 = successive_inner_merge(los_org_dfs)

# Perform the successive merges for tpia_org
tpia_org_3x3 = successive_inner_merge(tpia_org_dfs)

# Perform the successive merges for los_reg
los_reg_3x3 = successive_inner_merge(los_reg_dfs)

# Perform the successive merges for tpia_reg
tpia_reg_3x3 = successive_inner_merge(tpia_reg_dfs)



from pyspark.sql import functions as F

# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.union(los_org_21).union(los_org_22_a)
    .join(los_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
)

# Concatenate, merge, and rename for Tpia Org
tpia_org_all_yr_b = (
    tpia_org_20.union(tpia_org_21).union(tpia_org_22_a)
    .join(tpia_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
)

# Concatenate, merge and rename for los_reg
los_reg_all_yr_b = (
    los_reg_20.union(los_reg_21).union(los_reg_22_a)
    .join(los_reg_3x3.select('REGION_ID'), on='REGION_ID', how='inner')
    .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
)

# Concatenate, merge and rename for TPIA Reg
tpia_reg_all_yr_b = (
    tpia_reg_20.union(tpia_reg_21).union(tpia_reg_22_a)
    .join(tpia_reg_3x3.select('REGION_ID'), on='REGION_ID', how='inner')
    .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
)

