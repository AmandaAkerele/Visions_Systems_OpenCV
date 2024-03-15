from pyspark.sql import functions as F

# Calculate percentile for TPIA
percentiles_tpia = tpia_org_ta.stat.approxQuantile("PERCENTILE_90", [0.2, 0.8], 0.01)
prct_20_80_tpia_org_22_ta = spark.createDataFrame([(corp, percentiles_tpia[0], percentiles_tpia[1]) for corp in tpia_org_ta.select("CORP_PEER").distinct()],
                                                  ["CORP_PEER", "20th_Percentile", "80th_Percentile"])

# Calculate percentile for ELOS
percentiles_los = los_org_ta.stat.approxQuantile("PERCENTILE_90", [0.2, 0.8], 0.01)
prct_20_80_los_org_22_ta = spark.createDataFrame([(corp, percentiles_los[0], percentiles_los[1]) for corp in los_org_ta.select("CORP_PEER").distinct()],
                                                  ["CORP_PEER", "20th_Percentile", "80th_Percentile"])
