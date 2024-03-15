from pyspark.sql.functions import expr

# Calculate percentile for TPIA
prct_20_80_tpia_org_22_ta = tpia_org_ta.groupBy('CORP_PEER') \
    .agg(expr('percentile_approx(PERCENTILE_90, array(0.2, 0.8))').alias('percentiles')) \
    .selectExpr('CORP_PEER', 'percentiles[0] as 20th_Percentile', 'percentiles[1] as 80th_Percentile')

# Calculate percentile for ELOS
prct_20_80_los_org_22_ta = los_org_ta.groupBy('CORP_PEER') \
    .agg(expr('percentile_approx(PERCENTILE_90, array(0.2, 0.8))').alias('percentiles')) \
    .selectExpr('CORP_PEER', 'percentiles[0] as 20th_Percentile', 'percentiles[1] as 80th_Percentile')
