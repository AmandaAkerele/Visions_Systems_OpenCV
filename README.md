from pyspark.sql.functions import col

# Assuming ed_records_22_aa_df is created from a join of ed_nodup_noucc_nosb_22 and df_fac
# and you need to handle potential duplicate 'SUBMISSION_FISCAL_YEAR' column

# Alias DataFrames
ed_alias = ed_nodup_noucc_nosb_22.alias("ed")
fac_alias = df_fac.alias("fac")

# Perform the join using aliases
ed_records_22_aa_df = ed_alias.join(
    fac_alias,
    col("ed.FACILITY_AM_CARE_NUM") == col("fac.FACILITY_AM_CARE_NUM"),
    "left"
)

# Select columns and handle duplicates
# List all columns from ed_nodup_noucc_nosb_22 and selectively from df_fac to avoid duplicates
selected_columns = [col(f"ed.{column_name}") for column_name in ed_nodup_noucc_nosb_22.columns]

# Add columns from df_fac, renaming those that conflict
conflicting_columns = ['FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR']  # Include 'SUBMISSION_FISCAL_YEAR' here
for column_name in df_fac.columns:
    if column_name not in conflicting_columns:
        selected_columns.append(col(f"fac.{column_name}"))
    else:
        # If the column is conflicting, rename it (e.g., append '_fac')
        selected_columns.append(col(f"fac.{column_name}").alias(f"{column_name}_fac"))

# Construct the final DataFrame with selected columns
ed_records_22_aa_df = ed_records_22_aa_df.select(*selected_columns)

# LOS for admit without UCC
ed_records_admit_22_bb_df = ed_records_22_aa_df.filter(col('VISIT_DISPOSITION').isin(['06', '07']))
