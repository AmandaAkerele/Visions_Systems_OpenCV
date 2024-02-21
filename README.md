# LOS for admit without UCC
ed_records_admit_22_bb_df = ed_records_22_aa_df.filter(col('VISIT_DISPOSITION').isin(['06', '07']))

Note: Using this method of code below drop duplicate SUBMITTION_FISCAL_YEAR in the code above. Follow method of code below and functionality to solve this issue. 

from pyspark.sql.functions import col

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
conflicting_columns = ['FACILITY_AM_CARE_NUM']  # Add any other conflicting columns here
for column_name in df_fac.columns:
    if column_name not in conflicting_columns:
        selected_columns.append(col(f"fac.{column_name}"))
    else:
        # If the column is conflicting, rename it (e.g., append '_fac')
        selected_columns.append(col(f"fac.{column_name}").alias(f"{column_name}_fac"))

# Construct the final DataFrame with selected columns
ed_records_22_aa_df = ed_records_22_aa_df.select(*selected_columns)

