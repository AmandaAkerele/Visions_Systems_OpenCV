from pyspark.sql.functions import col

# Define a list of columns to keep
columns_to_keep = ['SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM', 'SITE_ID', 'SITE_NAME', 'SITE_PEER', 'CORP_ID', 
                   'CORP_NAME', 'CORP_PEER', 'REGION_ID', 'REGION_NAME', 'REGION_NAME', 'PROVINCE_ID', 'PROVINCE_NAME',
                   'MUNICIPALITY', 'POSTAL_CODE', 'INSTNUM', 'NACRS_ED_FLG', 'CLOSED_FLAG_ED', 'FIRST_YEAR_ED', 
                   'NEW_REGION_ID', 'REGION_E_DESC']

# Alias the datasets
df_fac_alias = df_fac.alias("fac")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Filter ed_facility_org for specific conditions and alias the result
ed_facility_org_filtered = ed_facility_org_alias.filter(
    (col("edfo.TYPE") == 'SL') & (col("edfo.CORP_CNT") == 1)
).select("edfo.CORP_ID").distinct()

# Perform the join operation and remove duplicates
ed_nacrs_flg_1_22 = df_fac_alias.join(
    ed_facility_org_filtered, 
    col("fac.CORP_ID") == col("edfo.CORP_ID")
).filter(col("fac.NACRS_ED_FLG") == 1).dropDuplicates(["fac.CORP_ID"])

# Select columns and handle duplicates
selected_columns = [col(f"fac.{column_name}") for column_name in columns_to_keep]

# Add columns from ed_facility_org_alias
conflicting_columns = ['CORP_ID']
for column_name in ed_nacrs_flg_1_22.columns: 
    if column_name not in conflicting_columns:
        selected_columns.append(col(f"edfo.{column_name}").alias(f"edfo_{column_name}"))

# Construct the final DataFrame with the selected columns
ed_nacrs_flg_1_22 = ed_nacrs_flg_1_22.select(selected_columns)
