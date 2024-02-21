# For Peer Level 
from pyspark.sql import functions as F

# Create 'tpia_rec' column with conditions
tpia_peer_cnt_a_df = ed_record_22_Peer.withColumn(
    'tpia_rec', 
    F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B')
     .when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
     .otherwise('Y')
)

# Filter out rows with 'tpia_rec' equal to 'B'
tpia_peer_cnt_df = tpia_peer_cnt_a_df.filter(F.col('tpia_rec') != 'B')

# Aggregating data for tpia_peer_rec_df
tpia_peer_rec_df = tpia_peer_cnt_df.groupBy('SUBMISSION_FISCAL_YEAR', 'SITE_PEER').agg(
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn('tpia_rec_pct', F.col('tpia_calc_cnt') / F.count(F.lit(1)))

# Creating tpia_supp_peer_df Dataframe
tpia_supp_peer_df = tpia_peer_rec_df.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)


For Region
from pyspark.sql import functions as F
import numpy as np

# Create 'tpia_rec' column with conditions
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    'tpia_rec',
    F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B')
     .when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
     .otherwise('Y')
)

# Filter out rows with 'tpia_rec' equal to 'B'
tpia_reg_cnt = ed_record_with_ucc_22.filter(F.col('tpia_rec') != 'B')

# Aggregating data for tpia_reg_rec
tpia_reg_rec = tpia_reg_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'NEW_REGION_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt'),
    (F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)) / F.count(F.lit(1))).alias('tpia_rec_pct')
)

# Creating tpia_supp_reg and tpia_rpt_reg Dataframe using filter
tpia_supp_reg = tpia_reg_rec.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)
tpia_rpt_reg = tpia_reg_rec.filter(
    (F.col('tpia_calc_cnt') >= 50) | ((F.col('tpia_calc_cnt') < 50) & (F.col('tpia_rec_pct') >= 0.75))
)

# Save suppression reports for province 
tpia_rpt_reg_v1_df = tpia_rpt_reg.join(df_fac, tpia_rpt_reg['NEW_REGION_ID'] == df_fac['NEW_REGION_ID'], 'left').dropDuplicates()
tpia_supp_reg_v1_df = tpia_supp_reg.join(df_fac, tpia_supp_reg['NEW_REGION_ID'] == df_fac['NEW_REGION_ID'], 'left').dropDuplicates()
tpia_rpt_org_v1_df = tpia_rpt_org.join(df_fac, tpia_rpt_org['CORP_ID'] == df_fac['CORP_ID'], 'left').dropDuplicates()
tpia_supp_org_v1_df = tpia_supp_org.join(df_fac, tpia_supp_org['CORP_ID'] == df_fac['CORP_ID'], 'left').dropDuplicates()

