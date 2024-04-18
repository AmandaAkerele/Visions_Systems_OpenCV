from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, min as spark_min, count, ceil, row_number
from pyspark.sql.window import Window

# Initialize the Spark session
spark = SparkSession.builder.appName("Percentile Calculation").getOrCreate()

# Define a window partitioned by the necessary groups and ordered by LOS_HOURS
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").orderBy("LOS_HOURS")

# Add a row number over the window to get distinct ranks
ranked = ed_record_admit_with_ucc_22.withColumn("rank", row_number().over(windowSpec))

# Calculate the total number of entries per group and the ceiling of the 90th percentile rank
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").agg(
    count("LOS_HOURS").alias("total"),
    ceil(0.9 * count("LOS_HOURS")).alias("ninety_pct_rank")  # Exact rank for the 90th percentile
)

# Join back on the original DataFrame to filter to approximately the 90th percentile
ninety_pct3 = ranked.join(
    total_counts,
    (ranked.SUBMISSION_FISCAL_YEAR == total_counts.SUBMISSION_FISCAL_YEAR) &
    (ranked.NEW_REGION_ID == total_counts.NEW_REGION_ID) &
    (ranked.REGION_E_DESC == total_counts.REGION_E_DESC)
)\
.filter(ranked.rank == total_counts.ninety_pct_rank)\
.groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC")\
.agg(spark_min("LOS_HOURS").alias("90th_Percentile_LOS"))

# Ensure the column is cast to double to perform rounding
ninety_pct3 = ninety_pct3.withColumn("90th_Percentile_LOS", col("90th_Percentile_LOS").cast("double"))

# Round the 90th Percentile LOS to two decimal places
ninety_pct3 = ninety_pct3.withColumn("90th_Percentile_LOS", round("90th_Percentile_LOS", 2))

# Show the result with appropriate formatting
ninety_pct3.show(truncate=False, vertical=True)
