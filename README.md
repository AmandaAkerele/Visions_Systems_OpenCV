from pyspark.sql.functions import col, count

# Create standalone DataFrame
stand_alone = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select('CORP_ID').distinct()

# Create multiple_sites DataFrame
multiple_sites = df_fac.join(stand_alone, 'CORP_ID', 'inner')
multiple_sites = multiple_sites.groupBy(df_fac['CORP_ID']).agg(count('*').alias('cnt_sites'))
multiple_sites = multiple_sites.filter(col('cnt_sites') > 1)

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
    df_fac_alias['CORP_ID'] == ed_facility_org_filtered['CORP_ID']
).filter(df_fac_alias['NACRS_ED_FLG'] == 1)

# Alias the DataFrames
tpia_org_22_alias = tpia_org_22.alias("tpia22")
ed_nacrs_flg_1_22_alias = ed_nacrs_flg_1_22.alias("ednacrs22")
tpia_supp_org_alias = tpia_supp_org.alias("tpiasupp")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Prepare the set of conditions for filtering
tpia_corp_conditions = (
    ~col("tpia22.CORP_ID").isin([row.CORP_ID for row in ed_nacrs_flg_1_22_alias.select("CORP_ID").collect()]) &
    ~col("tpia22.CORP_ID").isin([row.CORP_ID for row in tpia_supp_org_alias.select("CORP_ID").collect()]) &
    ~col("tpia22.CORP_ID").isin(
        [row.CORP_ID for row in ed_facility_org_alias.filter(
            (col("edfo.SUBMISSION_FISCAL_YEAR") == "2022") &
            ((col("edfo.TYPE") == 'PS') & (col("edfo.CORP_CNT") == 1) |
             (col("edfo.TYPE") == 'DQ') & (col("edfo.CORP_CNT") == 1) & (col("edfo.IND") == 'TPIA'))
        ).select("edfo.CORP_ID").collect()]
    )
)

# Apply the filter conditions
tpia_org_ta = tpia_org_22_alias.filter(tpia_corp_conditions)



or


from pyspark.sql.functions import col, count

# ... (previous code for creating stand_alone and multiple_sites DataFrames)

# Alias the DataFrames
df_fac_alias = df_fac.alias("fac")
ed_facility_org_alias = ed_facility_org.alias("edfo")
tpia_org_22_alias = tpia_org_22.alias("tpia22")
ed_nacrs_flg_1_22_alias = ed_nacrs_flg_1_22.alias("ednacrs22")
tpia_supp_org_alias = tpia_supp_org.alias("tpiasupp")

# Filter ed_facility_org for specific conditions and alias the result
ed_facility_org_filtered = ed_facility_org_alias.filter(
    (col("edfo.TYPE") == 'SL') & (col("edfo.CORP_CNT") == 1)
).select("edfo.CORP_ID")

# Perform the join operation
ed_nacrs_flg_1_22 = df_fac_alias.join(
    ed_facility_org_filtered, 
    col("fac.CORP_ID") == col("edfo.CORP_ID")
).filter(col("fac.NACRS_ED_FLG") == 1)

# Create a DataFrame of excluded CORP_IDs from ed_nacrs_flg_1_22 and tpia_supp_org
excluded_corp_ids = ed_nacrs_flg_1_22_alias.select("CORP_ID").distinct() \
    .union(tpia_supp_org_alias.select("CORP_ID").distinct())

# Further exclusion based on 'SUBMISSION_FISCAL_YEAR', 'TYPE', 'CORP_CNT', and 'IND'
additional_exclusions = ed_facility_org_alias.filter(
    (col("edfo.SUBMISSION_FISCAL_YEAR") == "2022") &
    ((col("edfo.TYPE") == 'PS') & (col("edfo.CORP_CNT") == 1) |
     (col("edfo.TYPE") == 'DQ') & (col("edfo.CORP_CNT") == 1) & (col("edfo.IND") == 'TPIA'))
).select("edfo.CORP_ID").distinct()

excluded_corp_ids = excluded_corp_ids.union(additional_exclusions).distinct()

# Apply the filter conditions
tpia_org_ta = tpia_org_22_alias.join(excluded_corp_ids, "CORP_ID", "left_anti")

