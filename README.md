from pyspark.sql import functions as F

# Assume ed_record_22_Peer is your starting DataFrame
tpia_peer_cnt_a_df = ed_record_22_Peer.withColumn('tpia_rec', F.lit('B'))

# Update tpia_rec based on conditions in TIME_PHYSICAN_INIT_ASSESSMENT column
tpia_peer_cnt_a_df = tpia_peer_cnt_a_df.withColumn(
    'tpia_rec',
    F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == '', 'B') \
    .when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N') \
    .otherwise('Y')
)

# Filter out records where tpia_rec is not 'B'
tpia_peer_cnt_df = tpia_peer_cnt_a_df.filter(F.col('tpia_rec') != 'B')

# Group by and aggregate calculations
tpia_peer_rec_df = tpia_peer_cnt_df.groupBy('SUBMISSION_FISCAL_YEAR', 'SITE_PEER').agg(
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn(
    'tpia_rec_pct',
    (F.col('tpia_calc_cnt') / F.col('tpia_elig_cnt'))
)

# Filter for suppression check
tpia_supp_peer_df = tpia_peer_rec_df.filter(F.col('tpia_calc_cnt') < 50)

# Check if DataFrames are empty and print message if they are
tpia_peer_rec_df_count = tpia_peer_rec_df.count()
tpia_supp_peer_df_count = tpia_supp_peer_df.count()

if tpia_peer_rec_df_count == 0:
    print("No suppression at peer level.")

if tpia_supp_peer_df_count == 0:
    print("No suppression at peer level.")
