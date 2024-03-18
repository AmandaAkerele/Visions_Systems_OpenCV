# Calculate percentiles for ELOS_org
prct_20_80_los_org_22_ta = los_org_ta \
    .groupBy("CORP_PEER") \
    .agg(expr("percentile(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile(PERCENTILE_90, 0.8)").alias("80th_Percentile"))
