# Alias the datasets 

df_fac_alias = df_fac.alias("fac")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Filter ed_facility_org for specific conditions and alias the result
ed_facility_org_filtered = ed_facility_org_alias.filter(
    (col("edfo.TYPE") == 'SL') & (col("edfo.CORP_CNT") == 1)
).select("edfo.CORP_ID")

# Perform the join operation

ed_nacrs_flg_1_22 = df_fac_alias.join(
    ed_facility_org_filtered, 
    col("fac.CORP_ID") == col("edfo.CORP_ID")
).filter(col("fac.NACRS_ED_FLG") == 1)
    


# Select columns and handle duplicates
# List all columns from df_fac and selectively from df_dq to avoid duplicates
selected_columns = [col(f"fac.{column_name}") for column_name in df_fac.columns]

# Add columns from df_dq, renaming those that conflict
conflicting_columns = ['SITE_ID', 'FACILITY_AM_CARE_NUM', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 'REGION_NAME']  # Adjust based on your data
for column_name in df_dq.columns:
    if column_name not in conflicting_columns:
        selected_columns.append(col(f"dq.{column_name}"))

# Construct the final DataFrame with selected columns
merged_df = merged_df.select(selected_columns)

# Define a list of columns to keep
columns_to_keep = [
    'FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'CORP_ID',
    'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG'
]

# Add 'TYPE' column with values 'DQ' for merged_df
merged_df = merged_df.withColumn('TYPE', lit('DQ'))




# Create DataFrames t3 and t4 based on conditions
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1) \
          .select(*columns_to_keep) \
          .withColumn('TYPE', lit('SL')) \
          .withColumn('IND', lit(''))

# Handle conflicting column names for t4
selected_columns_t4 = [col(f"fac.{column_name}") for column_name in columns_to_keep]

# Add columns from df_ps, renaming those that conflict
conflicting_columns_t4 = ['FACILITY_AM_CARE_NUM']  # Adjust based on your data
for column_name in df_ps.columns:
    if column_name not in conflicting_columns_t4:
        selected_columns_t4.append(col(f"ps.{column_name}"))
