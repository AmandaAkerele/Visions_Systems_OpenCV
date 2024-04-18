from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Site Percentile Calculation Enhanced").getOrCreate()

windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").orderBy("LOS_HOURS")
ranked = ed_record_admit_22.withColumn("rank", F.row_number().over(windowSpec))

# Improved aliasing for clearer references
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").agg(
    F.count("LOS_HOURS").alias("total"),
    F.ceil(0.9 * F.count("LOS_HOURS")).alias("ninety_pct_precise")
).alias("total_counts")

ranked = ranked.alias("ranked")

# Explicitly specify DataFrame aliases in join conditions
los_site_22 = ranked.join(
    total_counts,
    (col("ranked.SUBMISSION_FISCAL_YEAR") == col("total_counts.SUBMISSION_FISCAL_YEAR")) &
    (col("ranked.SITE_ID") == col("total_counts.SITE_ID")) &
    (col("ranked.SITE_NAME") == col("total_counts.SITE_NAME")) &
    (col("ranked.SITE_PEER") == col("total_counts.SITE_PEER"))
).filter(col("ranked.rank") == col("total_counts.ninety_pct_precise"))

# Use the DataFrame alias to specify which columns to group by
los_site_22 = los_site_22.groupBy(
    col("ranked.SUBMISSION_FISCAL_YEAR"),
    col("ranked.SITE_ID"),
    col("ranked.SITE_NAME"),
    col("ranked.SITE_PEER")
).agg(F.min(col("ranked.LOS_HOURS")).alias("PERCENTILE_90"))

# Custom rounding logic
los_site_22 = los_site_22.withColumn(
    "PERCENTILE_90", 
    F.when(
        col("PERCENTILE_90") - F.floor("PERCENTILE_90") < 0.06, 
        col("PERCENTILE_90") + 0.06
    ).otherwise(col("PERCENTILE_90"))
)

# Standard rounding to 2 decimal places
los_site_22 = los_site_22.withColumn("PERCENTILE_90", F.round("PERCENTILE_90", 2))

los_site_22.show(truncate=False)
