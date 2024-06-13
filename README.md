combine these codes to be one 

from pyspark.sql.functions import col, count, when

# Directly compute the number of years each CORP_ID was in the PERCENTILE_90
corp_count = tpia_org_combined.withColumn(
    "in_percentile_90", 
    when(col("PERCENTILE_90").isNotNull(), 1).otherwise(0)
).groupBy("CORP_ID").agg(
    count(when(col("in_percentile_90") == 1, True)).alias("years_in_percentile_90")
)

# Filter out CORP_IDs that have been in PERCENTILE_90 for at least 3 years
corp_filtered = corp_count.filter(col("years_in_percentile_90") >= 3)

# Join and select records for the last year
corp_comp = tpia_org_combined.join(
    corp_filtered, "CORP_ID", "inner"
).filter(col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))

# Similarly, select records for the last three years
corp_trend = tpia_org_combined.join(
    corp_filtered, "CORP_ID", "inner"
).filter(col("SUBMISSION_FISCAL_YEAR").isin([str(closed_year-1), str(closed_year-2), str(closed_year-3)]))

# Show the result
# corp_comp.show()
# corp_trend.show()


from pyspark.sql.functions import col, count, when

# Directly compute the number of years each CORP_ID was in the PERCENTILE_90
reg_count = tpia_reg_combined.withColumn(
    "in_percentile_90", 
    when(col("PERCENTILE_90").isNotNull(), 1).otherwise(0)
).groupBy("adm_region_id").agg(
    count(when(col("in_percentile_90") == 1, True)).alias("years_in_percentile_90")
)

# Filter out REG_IDs that have been in PERCENTILE_90 for at least 3 years
reg_filtered = reg_count.filter(col("years_in_percentile_90") >= 3)

# Join and select records for the last year
reg_comp = tpia_reg_combined.join(
    reg_filtered, "adm_region_id", "inner"
).filter(col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))

# Similarly, select records for the last three years
reg_trend = tpia_reg_combined.join(
    reg_filtered, "adm_region_id", "inner"
).filter(col("SUBMISSION_FISCAL_YEAR").isin([str(closed_year-1), str(closed_year-2), str(closed_year-3)]))

# Show the result
# reg_comp.show()
# reg_trend.show()
