los_site_22 = ed_record_admit_22.groupBy('SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))
los_site_22.show()

please make the code above resemble the code below:

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

# Properly alias all columns in the total_counts DataFrame to avoid ambiguity
total_counts = total_counts.select(
    F.col("SUBMISSION_FISCAL_YEAR").alias("total_SUBMISSION_FISCAL_YEAR"),
    F.col("NEW_REGION_ID").alias("total_NEW_REGION_ID"),
    F.col("REGION_E_DESC").alias("total_REGION_E_DESC"),
    "total",
    "ninety_pct_rank"
)

# Join back on the original DataFrame to filter to approximately the 90th percentile
cond = [
    ranked.SUBMISSION_FISCAL_YEAR == total_counts.total_SUBMISSION_FISCAL_YEAR,
    ranked.NEW_REGION_ID == total_counts.total_NEW_REGION_ID,
    ranked.REGION_E_DESC == total_counts.total_REGION_E_DESC
]

los_reg_22 = ranked.join(total_counts, cond)\
    .filter(ranked.rank == total_counts.ninety_pct_rank)\
    .groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC")\
    .agg(F.min("LOS_HOURS").alias("90th_Percentile_LOS"))

# Round the 90th_Percentile_LOS to two decimal places
los_reg_22 = los_reg_22.withColumn("90th_Percentile_LOS", F.format_number("90th_Percentile_LOS", 1))

# Show the result
los_reg_22.show()
