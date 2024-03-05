from pyspark.sql.functions import col

# Alias the datasets 
df_fac_alias = df_fac.alias("fac")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Filter ed_facility_org for specific conditions and alias the result
ed_facility_org_filtered = ed_facility_org_alias.filter(
    (col("edfo.TYPE") == 'SL') & (col("edfo.CORP_CNT") == 1)
).select(col("edfo.CORP_ID").alias("CORP_ID_edfo"))

# Perform the join operation
ed_nacrs_flg_1_22 = df_fac_alias.join(
    ed_facility_org_filtered, 
    col("fac.CORP_ID") == col("CORP_ID_edfo")
).filter(col("fac.NACRS_ED_FLG") == 1)

# Recreate duplicates
ed_nacrs_flg_1_22_with_duplicates = ed_nacrs_flg_1_22.union(ed_nacrs_flg_1_22.withColumnRenamed("CORP_ID", "CORP_ID_2"))

# Drop duplicates
ed_nacrs_flg_1_22 = ed_nacrs_flg_1_22_with_duplicates.dropDuplicates()
