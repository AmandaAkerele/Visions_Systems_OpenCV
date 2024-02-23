from pyspark.sql.functions import col

# Exclusion Rule for Region, Province, and National
tpia_org_supp_for_natprovreg_df = tpia_supp_org_ucc_22.filter(col('tpia_calc_cnt') < 5)

# Collect the list of CORP_IDs to be excluded
exclude_corp_ids = tpia_org_supp_for_natprovreg_df.select('CORP_ID').rdd.flatMap(lambda x: x).collect()

# Filter out the rows from ed_record_with_ucc_22
TPIA_nt_record_ucc_df = ed_record_with_ucc_22.filter(~col('CORP_ID').isin(exclude_corp_ids))
