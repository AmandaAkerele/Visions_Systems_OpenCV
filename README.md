from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType

# Define a Python function to compute percentiles
def compute_percentiles(percentiles_list, q1, q2):
    percentiles_list.sort()
    return percentiles_list[int(q1 * len(percentiles_list))], percentiles_list[int(q2 * len(percentiles_list))]

# Register the Python function as a UDF
compute_percentiles_udf = udf(compute_percentiles, ArrayType(FloatType()))

# Calculate percentiles for TPIA
prct_20_80_tpia_org_22_ta = percentiles_tpia.select(
    "CORP_PEER",
    compute_percentiles_udf("percentiles", lit(0.2), lit(0.8)).alias("percentiles")
).select(
    "CORP_PEER",
    col("percentiles")[0].alias("20th_Percentile"),
    col("percentiles")[1].alias("80th_Percentile")
)

# Calculate percentiles for ELOS
prct_20_80_los_org_22_ta = percentiles_los.select(
    "CORP_PEER",
    compute_percentiles_udf("percentiles", lit(0.2), lit(0.8)).alias("percentiles")
).select(
    "CORP_PEER",
    col("percentiles")[0].alias("20th_Percentile"),
    col("percentiles")[1].alias("80th_Percentile")
)



orr


from pyspark.sql.functions import expr

# Calculate percentiles for TPIA
prct_20_80_tpia_org_22_ta = tpia_org_ta.groupBy('CORP_PEER') \
    .agg(expr('percentile_approx(PERCENTILE_90, 0.2), percentile_approx(PERCENTILE_90, 0.8)').alias('percentiles')) \
    .selectExpr('CORP_PEER', 'percentiles[0] as 20th_Percentile', 'percentiles[1] as 80th_Percentile')

# Calculate percentiles for ELOS
prct_20_80_los_org_22_ta = los_org_ta.groupBy('CORP_PEER') \
    .agg(expr('percentile_approx(PERCENTILE_90, 0.2), percentile_approx(PERCENTILE_90, 0.8)').alias('percentiles')) \
    .selectExpr('CORP_PEER', 'percentiles[0] as 20th_Percentile', 'percentiles[1] as 80th_Percentile')
