from pyspark.sql.functions import col

# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.unionByName(los_org_21, allowMissingColumns=True)
               .unionByName(los_org_22_a, allowMissingColumns=True)
               .join(los_org_3x3, on='CORP_ID', how='inner')
               .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Alias both DataFrames
los_org_all_yr_b_alias = los_org_all_yr_b.alias("los_org")
df_dq_alias = df_dq.alias("dq")

# Perform the inner join using aliases
tmp_ed_facility_org_a = los_org_all_yr_b_alias.join(
    df_dq_alias,
    (col("los_org.CORP_ID") == col("dq.CORP_ID")) &
    (col("los_org.TIME") == col("dq.FISCAL_YEAR")),
    'inner'
)

# Select columns and handle duplicates
# List all columns from los_org_all_yr_b and selectively from df_dq to avoid duplicates
selected_columns = [col(f"los_org.{column_name}") for column_name in los_org_all_yr_b.columns]

# Add columns from df_dq, renaming those that conflict
conflicting_columns = ['SITE_ID', 'FACILITY_AM_CARE_NUM', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 'REGION_NAME', 'CORP_PEER']  # Adjust based on your data for future use
for column_name in df_dq.columns:
    if column_name not in conflicting_columns:
        selected_columns.append(col(f"dq.{column_name}"))

# Construct the final DataFrame with selected columns
tmp_ed_facility_org_a = tmp_ed_facility_org_a.select(selected_columns)
