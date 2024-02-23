from pyspark.sql import functions as F

# Continue with your previous steps...

# Aggregating data for tpia_peer_rec_df
tpia_peer_rec_df = tpia_peer_cnt_df.groupBy('SUBMISSION_FISCAL_YEAR', 'SITE_PEER').agg(
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt'),
    (F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)) / F.count(F.lit(1))).alias('tpia_rec_pct')
)

# Creating tpia_supp_peer_df DataFrame
tpia_supp_peer_df = tpia_peer_rec_df.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)
