from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Assuming SparkSession has already been created and named 'spark'

# Directly compute the number of years each CORP_ID was in the PERCENTILE_90
corp_count = corp_all.withColumn(
    "in_percentile_90", 
    when(col("PERCENTILE_90").isNotNull(), 1).otherwise(0)
).groupBy("CORP_ID").agg(
    count(when(col("in_percentile_90") == 1, True)).alias("years_in_percentile_90")
)

# Filter out CORP_IDs that have been in PERCENTILE_90 for at least 3 years
corp_filtered = corp_count.filter(col("years_in_percentile_90") >= 3)

# Join and select records for the last year
corp_comp = corp_all.join(
    corp_filtered, "CORP_ID", "inner"
).filter(col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))

# Similarly, select records for the last three years
corp_trend = corp_all.join(
    corp_filtered, "CORP_ID", "inner"
).filter(col("SUBMISSION_FISCAL_YEAR").isin([str(closed_year-1), str(closed_year-2), str(closed_year-3)]))

# Show the result
# corp_comp.show()
# corp_trend.show()
