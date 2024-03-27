from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col

# Define columns to keep for union operation
columns_to_keep = ['CORP_ID', 'CORP_PEER']  # Adjust based on your data

# Select only the columns to keep from each dataframe
los_org_20_selected = los_org_20.select(columns_to_keep)
los_org_21_selected = los_org_21.select(columns_to_keep)
los_org_22_a_selected = los_org_22_a.select(columns_to_keep)

# Union the dataframes
los_org_union = los_org_20_selected.union(los_org_21_selected).union(los_org_22_a_selected)

# Join with los_org_3x3 and rename column
los_org_all_yr_b = (
    los_org_union
    .join(los_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
)

# Similarly adjust for other datasets...




# Define columns to keep for tpia_org
tpia_org_columns_to_keep = ['CORP_ID', 'CORP_PEER']  # Adjust based on your data

# Select only the columns to keep from each dataframe for tpia_org
tpia_org_20_selected = tpia_org_20.select(tpia_org_columns_to_keep)
tpia_org_21_selected = tpia_org_21.select(tpia_org_columns_to_keep)
tpia_org_22_a_selected = tpia_org_22_a.select(tpia_org_columns_to_keep)

# Union the dataframes for tpia_org
tpia_org_union = tpia_org_20_selected.union(tpia_org_21_selected).union(tpia_org_22_a_selected)

# Join with tpia_org_3x3 and rename column
tpia_org_all_yr_b = (
    tpia_org_union
    .join(tpia_org_3x3.select('CORP_ID'), on='CORP_ID', how='inner')
    .withColumnRenamed('FISCAL_YEAR', 'TIME')
)

# Define columns to keep for los_reg
los_reg_columns_to_keep = ['REGION_ID']  # Adjust based on your data

# Select only the columns to keep from each dataframe for los_reg
los_reg_20_selected = los_reg_20.select(los_reg_columns_to_keep)
los_reg_21_selected = los_reg_21.select(los_reg_columns_to_keep)
los_reg_22_a_selected = los_reg_22_a.select(los_reg_columns_to_keep)

# Union the dataframes for los_reg
los_reg_union = los_reg_20_selected.union(los_reg_21_selected).union(los_reg_22_a_selected)

# Join with los_reg_3x3 and rename column
los_reg_all_yr_b = (
    los_reg_union
    .join(los_reg_3x3.select('REGION_ID'), on='REGION_ID', how='inner')
    .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
)

# Define columns to keep for tpia_reg
tpia_reg_columns_to_keep = ['REGION_ID']  # Adjust based on your data

# Select only the columns to keep from each dataframe for tpia_reg
tpia_reg_20_selected = tpia_reg_20.select(tpia_reg_columns_to_keep)
tpia_reg_21_selected = tpia_reg_21.select(tpia_reg_columns_to_keep)
tpia_reg_22_a_selected = tpia_reg_22_a.select(tpia_reg_columns_to_keep)

# Union the dataframes for tpia_reg
tpia_reg_union = tpia_reg_20_selected.union(tpia_reg_21_selected).union(tpia_reg_22_a_selected)

# Join with tpia_reg_3x3 and rename column
tpia_reg_all_yr_b = (
    tpia_reg_union
    .join(tpia_reg_3x3.select('REGION_ID'), on='REGION_ID', how='inner')
    .withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'TIME')
)



