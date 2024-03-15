
from pyspark.sql.functions import expr, percent_rank
from pyspark.sql.window import Window

# Define the percentiles
percentiles = [0.2, 0.8]

# Calculate percentiles for TPIA
window_spec = Window.partitionBy("CORP_PEER").orderBy("PERCENTILE_90")
prct_20_80_tpia_org_22_ta = tpia_org_ta \
    .withColumn("percentile_rank", percent_rank().over(window_spec)) \
    .filter(expr(f"percentile_rank in {percentiles}")) \
    .groupBy("CORP_PEER") \
    .pivot("percentile_rank", percentiles) \
    .agg(expr("approx_percentile(PERCENTILE_90, percentile)").alias("PERCENTILE_90"))

# Calculate percentiles for ELOS
prct_20_80_los_org_22_ta = los_org_ta \
    .withColumn("percentile_rank", percent_rank().over(window_spec)) \
    .filter(expr(f"percentile_rank in {percentiles}")) \
    .groupBy("CORP_PEER") \
    .pivot("percentile_rank", percentiles) \
    .agg(expr("approx_percentile(PERCENTILE_90, percentile)").alias("PERCENTILE_90"))


orrrrr


# Calculate percentiles for TPIA
percentiles_tpia = tpia_org_ta.groupBy("CORP_PEER").agg(expr("collect_list(PERCENTILE_90) as percentiles"))

# Calculate percentiles for ELOS
percentiles_los = los_org_ta.groupBy("CORP_PEER").agg(expr("collect_list(PERCENTILE_90) as percentiles"))

# Define a function to compute percentiles
def compute_percentiles(percentiles_list):
    percentiles_list.sort()
    return percentiles_list[int(0.2 * len(percentiles_list))], percentiles_list[int(0.8 * len(percentiles_list))]

# Compute percentiles for TPIA
prct_20_80_tpia_org_22_ta = percentiles_tpia.select(
    "CORP_PEER",
    expr("transform(percentiles, x -> percentile(x, 0.2))")[0].alias("20th_Percentile"),
    expr("transform(percentiles, x -> percentile(x, 0.8))")[0].alias("80th_Percentile")
)

# Compute percentiles for ELOS
prct_20_80_los_org_22_ta = percentiles_los.select(
    "CORP_PEER",
    expr("transform(percentiles, x -> percentile(x, 0.2))")[0].alias("20th_Percentile"),
    expr("transform(percentiles, x -> percentile(x, 0.8))")[0].alias("80th_Percentile")
)
