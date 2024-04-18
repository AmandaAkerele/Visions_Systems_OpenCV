from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Enhanced Percentile Calculation").getOrCreate()

# Define a window partitioned by the necessary groups and ordered by LOS_HOURS
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "CORP_ID", "CORP_NAME", "CORP_PEER").orderBy("LOS_HOURS")

# Add a row number over the window for distinct ranks
ranked = ed_record_admit_22.withColumn("rank", F.row_number().over(windowSpec))

# Calculate the total number of entries per group and the ceiling of the 90th percentile rank
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "CORP_ID", "CORP_NAME", "CORP_PEER").agg(
    F.count("LOS_HOURS").alias("total"),
    F.ceil(0.9 * F.count("LOS_HOURS")).alias("ninety_pct_rank")
)

# Alias all columns in the total_counts DataFrame to avoid ambiguity
total_counts = total_counts.select(
    F.col("SUBMISSION_FISCAL_YEAR").alias("total_SUBMISSION_FISCAL_YEAR"),
    F.col("CORP_ID").alias("total_CORP_ID"),
    F.col("CORP_NAME").alias("total_CORP_NAME"),
    F.col("CORP_PEER").alias("total_CORP_PEER"),
    "total",
    "ninety_pct_rank"
)

# Join conditions to combine data frames without ambiguity
cond = [
    ranked.SUBMISSION_FISCAL_YEAR == total_counts.total_SUBMISSION_FISCAL_YEAR,
    ranked.CORP_ID == total_counts.total_CORP_ID,
    ranked.CORP_NAME == total_counts.total_CORP_NAME,
    ranked.CORP_PEER == total_counts.total_CORP_PEER
]

# Join the dataframes based on the conditions and filter to the approximate 90th percentile
los_org_22 = ranked.join(total_counts, cond)\
    .filter(ranked.rank == total_counts.ninety_pct_rank)\
    .groupBy("SUBMISSION_FISCAL_YEAR", "CORP_ID", "CORP_NAME", "CORP_PEER")\
    .agg(F.min("LOS_HOURS").alias("PERCENTILE_90"))

# Filter rows based on specific 'SITE_ID' values and select specific columns for union operation
filtered_rows_los = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209])).select(
    col('SUBMISSION_FISCAL_YEAR'),
    col('SITE_ID').alias('CORP_ID'),
    col('SITE_NAME').alias('CORP_NAME'),
    col('SITE_PEER').alias('CORP_PEER'),
    col('90th_Percentile_LOS').alias('PERCENTILE_90')
)

# Union the datasets using unionByName with allowMissingColumns=True
los_org_22 = los_org_22.unionByName(filtered_rows_los, allowMissingColumns=True)

# Round the PERCENTILE_90 to two decimal places for consistency in display
los_org_22 = los_org_22.withColumn("PERCENTILE_90", F.format_number("PERCENTILE_90", 2))

# Display the result
los_org_22.show(truncate=False)
