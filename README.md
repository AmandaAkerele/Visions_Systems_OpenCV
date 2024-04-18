from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Site Percentile Calculation").getOrCreate()

# Define window and ranking
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").orderBy("LOS_HOURS")
ranked = ed_record_admit_22.withColumn("rank", F.row_number().over(windowSpec))

# Increase accuracy of the percentile approximation by reducing the relative error
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").agg(
    F.count("LOS_HOURS").alias("total"),
    F.expr("percentile_approx(LOS_HOURS, 0.9, 100000)").alias("ninety_pct_precise")  # Using a larger sample size for accuracy
)

# Join and filter operations as before
cond = [
    ranked.SUBMISSION_FISCAL_YEAR == total_counts.total_SUBMISSION_FISCAL_YEAR,
    ranked.SITE_ID == total_counts.total_SITE_ID,
    ranked.SITE_NAME == total_counts.total_SITE_NAME,
    ranked.SITE_PEER == total_counts.total_SITE_PEER
]

los_site_22 = ranked.join(total_counts, cond)\
    .filter(ranked.rank == total_counts.ninety_pct_precise)\
    .groupBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER")\
    .agg(F.min("LOS_HOURS").alias("PERCENTILE_90"))

# Custom rounding logic: If last digit is less than 6, add 0.06
los_site_22 = los_site_22.withColumn(
    "PERCENTILE_90", 
    F.when(
        F.col("PERCENTILE_90") - F.floor("PERCENTILE_90") < 0.06, 
        F.col("PERCENTILE_90") + 0.06
    ).otherwise(F.col("PERCENTILE_90"))
)

# Standard rounding to 2 decimal places
los_site_22 = los_site_22.withColumn("PERCENTILE_90", F.round("PERCENTILE_90", 2))

los_site_22.show(truncate=False)
