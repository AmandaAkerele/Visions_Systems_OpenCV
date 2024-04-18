from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize the Spark session
spark = SparkSession.builder.appName("Site Percentile Calculation").getOrCreate()

# Define a window partitioned by the necessary groups and ordered by LOS_HOURS
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").orderBy("LOS_HOURS")

# Add a row number over the window for distinct ranks
ranked = ed_record_admit_22.withColumn("rank", F.row_number().over(windowSpec))

# Calculate the total number of entries per group and the ceiling of the 90th percentile rank
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").agg(
    F.count("LOS_HOURS").alias("total"),
    F.ceil(0.9 * F.count("LOS_HOURS")).alias("ninety_pct_rank")
)

# Properly alias all columns in the total_counts DataFrame to avoid ambiguity
total_counts = total_counts.select(
    F.col("SUBMISSION_FISCAL_YEAR").alias("total_SUBMISSION_FISCAL_YEAR"),
    F.col("SITE_ID").alias("total_SITE_ID"),
    F.col("SITE_NAME").alias("total_SITE_NAME"),
    F.col("SITE_PEER").alias("total_SITE_PEER"),
    "total",
    "ninety_pct_rank"
)

# Join back on the original DataFrame to filter to approximately the 90th percentile
cond = [
    ranked.SUBMISSION_FISCAL_YEAR == total_counts.total_SUBMISSION_FISCAL_YEAR,
    ranked.SITE_ID == total_counts.total_SITE_ID,
    ranked.SITE_NAME == total_counts.total_SITE_NAME,
    ranked.SITE_PEER == total_counts.total_SITE_PEER
]

los_site_22 = ranked.join(total_counts, cond)\
    .filter(ranked.rank == total_counts.ninety_pct_rank)\
    .groupBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER")\
    .agg(F.min("LOS_HOURS").alias("PERCENTILE_90"))

# Round the PERCENTILE_90 to two decimal places
los_site_22 = los_site_22.withColumn("PERCENTILE_90", F.format_number("PERCENTILE_90", 2))

# Show the result
los_site_22.show(truncate=False)
