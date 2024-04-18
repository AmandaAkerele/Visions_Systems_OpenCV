from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Percentile Calculation").getOrCreate()

# Define window specification
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").orderBy("LOS_HOURS")

# Ranking within each window
ranked = ed_record_admit_with_ucc_22.withColumn("rank", F.row_number().over(windowSpec))

# Calculate total entries and 90th percentile rank
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").agg(
    F.count("LOS_HOURS").alias("total"),
    F.ceil(0.9 * F.count("LOS_HOURS")).alias("ninety_pct_rank")
)

# Join and filter
ninety_pct4 = ranked.join(
    total_counts,
    (ranked.SUBMISSION_FISCAL_YEAR == total_counts.SUBMISSION_FISCAL_YEAR) &
    (ranked.NEW_REGION_ID == total_counts.NEW_REGION_ID) &
    (ranked.REGION_E_DESC == total_counts.REGION_E_DESC)
).filter(ranked.rank == total_counts.ninety_pct_rank)

# Group and aggregate
ninety_pct4 = ninety_pct4.groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").agg(
    F.min("LOS_HOURS").alias("90th_Percentile_LOS")
)

# Formatting as string with two decimal places
ninety_pct4 = ninety_pct4.withColumn("90th_Percentile_LOS", F.format_number("90th_Percentile_LOS", 2))

# Show results
ninety_pct4.show(truncate=False)
