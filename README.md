from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Refined Percentile Calculation").getOrCreate()

windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").orderBy("LOS_HOURS")
ranked = ed_record_admit_22.withColumn("rank", F.row_number().over(windowSpec))

# Using a higher accuracy setting in percentile_approx
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").agg(
    F.count("LOS_HOURS").alias("total"),
    F.percentile_approx("LOS_HOURS", 0.9, 1000000).alias("ninety_pct_precise")  # Increased accuracy
)

total_counts = total_counts.select(
    F.col("SUBMISSION_FISCAL_YEAR").alias("total_SUBMISSION_FISCAL_YEAR"),
    F.col("SITE_ID").alias("total_SITE_ID"),
    F.col("SITE_NAME").alias("total_SITE_NAME"),
    F.col("SITE_PEER").alias("total_SITE_PEER"),
    "total",
    "ninety_pct_precise"
)

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

# Custom rounding logic: adjust if close to the next higher decimal
los_site_22 = los_site_22.withColumn(
    "PERCENTILE_90",
    F.when(
        F.col("PERCENTILE_90") % 1 >= 0.1,  # Custom condition to adjust rounding
        F.ceil(F.col("PERCENTILE_90") * 10) / 10
    ).otherwise(
        F.round(F.col("PERCENTILE_90"), 1)
    )
)

los_site_22.show(truncate=False)
