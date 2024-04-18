from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define a window partitioned by the necessary groups and ordered by LOS_HOURS
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").orderBy("LOS_HOURS")

# Add a row number over the window
ranked = ed_record_admit_with_ucc_22.withColumn("rank", F.row_number().over(windowSpec))  # using row_number to get distinct ranks

# Calculate the total number of entries per group and the ceiling of the 90th percentile rank
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").agg(
    F.count("LOS_HOURS").alias("total"),
    F.ceil(0.9 * F.count("LOS_HOURS")).alias("ninety_pct_rank")  # Calculate the exact rank for the 90th percentile
)

# Join back on the original DataFrame to filter to approximately the 90th percentile
ninety_pct3 = ranked.join(
    total_counts,
    (ranked.SUBMISSION_FISCAL_YEAR == total_counts.SUBMISSION_FISCAL_YEAR) &
    (ranked.NEW_REGION_ID == total_counts.NEW_REGION_ID) &
    (ranked.REGION_E_DESC == total_counts.REGION_E_DESC)
)\
    .filter(ranked.rank == total_counts.ninety_pct_rank)\
    .groupBy(ranked.SUBMISSION_FISCAL_YEAR, ranked.NEW_REGION_ID, ranked.REGION_E_DESC)\
    .agg(F.min(ranked.LOS
