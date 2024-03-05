from pyspark.sql.functions import col

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

# Recreate duplicates
ed_nacrs_flg_1_22_with_duplicates = ed_nacrs_flg_1_22.union(ed_nacrs_flg_1_22)

# Drop duplicates
ed_nacrs_flg_1_22 = ed_nacrs_flg_1_22_with_duplicates.dropDuplicates()


or

from pyspark.sql.functions import col

# Alias the datasets 
df_fac_alias = df_fac.alias("fac")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Filter ed_facility_org for specific conditions and alias the result
ed_facility_org_filtered = ed_facility_org_alias.filter(
    (col("edfo.TYPE") == 'SL') & (col("edfo.CORP_CNT") == 1)
).select("edfo.CORP_ID").distinct()  # Adding distinct to remove duplicate CORP_IDs

# Perform the join operation
ed_nacrs_flg_1_22 = df_fac_alias.join(
    ed_facility_org_filtered, 
    col("fac.CORP_ID") == col("edfo.CORP_ID")
).filter(col("fac.NACRS_ED_FLG") == 1)

# Drop duplicates if needed
ed_nacrs_flg_1_22 = ed_nacrs_flg_1_22.dropDuplicates()
