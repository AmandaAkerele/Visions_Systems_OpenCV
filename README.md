from pyspark.sql.functions import expr

# Define a user-defined function (UDF) to calculate percentiles
def calculate_percentiles(values):
    sorted_values = sorted(values)
    percentile_20 = sorted_values[int(len(sorted_values) * 0.2)]
    percentile_80 = sorted_values[int(len(sorted_values) * 0.8)]
    return percentile_20, percentile_80

calculate_percentiles_udf = F.udf(calculate_percentiles, returnType="struct<double,double>")

# Calculate percentiles for TPIA
prct_20_80_tpia_org_22_ta = tpia_org_ta \
    .groupBy("CORP_PEER") \
    .agg(expr("percentile_approx(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile_approx(PERCENTILE_90, 0.8)").alias("80th_Percentile"))

# Calculate percentiles for ELOS
prct_20_80_los_org_22_ta = los_org_ta \
    .groupBy("CORP_PEER") \
    .agg(expr("percentile_approx(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile_approx(PERCENTILE_90, 0.8)").alias("80th_Percentile"))



orr 


from pyspark.sql.functions import expr

# Calculate percentiles for TPIA
prct_20_80_tpia_org_22_ta = tpia_org_ta \
    .groupBy("CORP_PEER") \
    .agg(expr("percentile(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile(PERCENTILE_90, 0.8)").alias("80th_Percentile"))

# Calculate percentiles for ELOS
prct_20_80_los_org_22_ta = los_org_ta \
    .groupBy("CORP_PEER") \
    .agg(expr("percentile(PERCENTILE_90, 0.2)").alias("20th_Percentile"),
         expr("percentile(PERCENTILE_90, 0.8)").alias("80th_Percentile"))


