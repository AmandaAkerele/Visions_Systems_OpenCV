from pyspark.sql.functions import expr

# Calculate percentiles for ELOS_reg
prct_20_80_los_reg_22_ta = los_reg_22_ta \
    .groupBy("SUBMISSION_FISCAL_YEAR") \
    .agg(expr("percentile(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile(PERCENTILE_90, 0.8)").alias("80th_Percentile"))

