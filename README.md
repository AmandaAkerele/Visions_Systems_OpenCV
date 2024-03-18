from pyspark.sql.functions import expr

# Calculate percentiles for ELOS_reg
prct_20_80_los_reg_22_ta = los_reg_22_ta.groupby("SUBMISSION_FISCAL_YEAR") \
    .agg(expr("percentile_approx(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile_approx(PERCENTILE_90, 0.8)").alias("80th_Percentile")) \
    .selectExpr("SUBMISSION_FISCAL_YEAR", "20th_Percentile", "80th_Percentile")
