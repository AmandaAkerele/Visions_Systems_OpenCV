from pyspark.sql import functions as F

# Rename columns for each DataFrame
renamed_columns = {
    '0.2': '20th_Percentile',
    '0.8': '80th_Percentile'
}

for df in [los_org_cmp, tpia_org_cmp, los_reg_cmp, tpia_reg_cmp]:
    for old_name, new_name in renamed_columns.items():
        df = df.withColumnRenamed(old_name, new_name)

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

# Apply conditions for each DataFrame
apply_conditions(los_org_cmp)
apply_conditions(tpia_org_cmp)
apply_conditions(los_reg_cmp)
apply_conditions(tpia_reg_cmp)






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

# Function to perform successive inner joins on a list of dataframes
def successive_inner_join(dataframes):
    result_df = dataframes[0]
    for df in dataframes[1:]:
        common_columns = set(result_df.columns).intersection(set(df.columns))
        join_columns = [col for col in result_df.columns if col in common_columns]
        result_df = result_df.join(df, join_columns, 'inner')
    return result_df

# Perform the successive joins for los_org
los_org_3x3 = successive_inner_join(los_org_dfs)

# Perform the successive joins for tpia_org
tpia_org_3x3 = successive_inner_join(tpia_org_dfs)

# Perform the successive joins for los_reg
los_reg_3x3 = successive_inner_join(los_reg_dfs)

# Perform the successive joins for tpia_reg
tpia_reg_3x3 = successive_inner_join(tpia_reg_dfs)
