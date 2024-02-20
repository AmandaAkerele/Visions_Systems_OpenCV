from pyspark.sql.functions import col

# Alias DataFrames
ed_nodup_noucc_nosb_alias = ed_nodup_noucc_nosb_22.alias("ed")
df_fac_alias = df_fac.alias("fac")

# Perform the join using aliases
ed_records_22_aa_df = ed_nodup_noucc_nosb_alias.join(
    df_fac_alias,
    col("ed.FACILITY_AM_CARE_NUM") == col("fac.FACILITY_AM_CARE_NUM"),
    "left"
)


# Select columns and handle duplicates
# Use the aliased column names to avoid ambiguity
selected_columns = [
    col("ed.AM_CARE_KEY"), 
    col("ed.SUBMISSION_FISCAL_YEAR").alias("SUBMISSION_FISCAL_YEAR"), 
    col("fac.FACILITY_PROVINCE"), 
    col("ed.FACILITY_AM_CARE_NUM"), 
    col("ed.TRIAGE_DATE"), 
    # Add other columns here as needed
    # Ensure you are using the aliased column names and resolving any conflicts
]

# Construct the final DataFrame with selected columns
ed_records_22_aa_df = ed_records_22_aa_df.select(*selected_columns)
