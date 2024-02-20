# # Alias both DataFrames
# df_fac_alias = df_fac.alias("fac")
# df_dq_alias = df_dq.alias("dq")

# # Perform the inner join using aliases
# tmp_ed_facility_org_a = df_fac_alias.join(
#     df_dq_alias,
#     (col("fac.FACILITY_AM_CARE_NUM") == col("dq.FACILITY_AM_CARE_NUM")) &
#     (col("fac.SUBMISSION_FISCAL_YEAR") == col("dq.FISCAL_YEAR")),
#     'inner'
# )

# # Select columns and handle duplicates
# # List all columns from df_fac and selectively from df_dq to avoid duplicates
# selected_columns = [col(f"fac.{column_name}") for column_name in df_fac.columns]

# # Add columns from df_dq, renaming those that conflict
# conflicting_columns = ['SITE_ID', 'FACILITY_AM_CARE_NUM', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 'REGION_NAME']  # Adjust based on your data
# for column_name in df_dq.columns:
#     if column_name not in conflicting_columns:
#         selected_columns.append(col(f"dq.{column_name}"))

# # Construct the final DataFrame with selected columns
# tmp_ed_facility_org_a = tmp_ed_facility_org_a.select(selected_columns)
