from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Organization Percentile Calculation").getOrCreate()

# Define window specification
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "CORP_ID", "CORP_NAME", "CORP_PEER").orderBy("LOS_HOURS")

# Ranking within each window
ranked = ed_record_admit_22.withColumn("rank", F.row_number().over(windowSpec))

# Calculate total entries and 90th percentile rank
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "CORP_ID", "CORP_NAME", "CORP_PEER").agg(
    F.count("LOS_HOURS").alias("total"),
    F.ceil(0.9 * F.count("LOS_HOURS")).alias("ninety_pct_rank")
)

# Join and filter
los_org_22 = ranked.join(
    total_counts,
    (ranked.SUBMISSION_FISCAL_YEAR == total_counts.SUBMISSION_FISCAL_YEAR) &
    (ranked.CORP_ID == total_counts.CORP_ID) &
    (ranked.CORP_NAME == total_counts.CORP_NAME) &
    (ranked.CORP_PEER == total_counts.CORP_PEER)
).filter(ranked.rank == total_counts.ninety_pct_rank)

# Group and aggregate
los_org_22 = los_org_22.groupBy("SUBMISSION_FISCAL_YEAR", "CORP_ID", "CORP_NAME", "CORP_PEER").agg(
    F.min("LOS_HOURS").alias("PERCENTILE_90")
)

# Filter rows based on specific 'SITE_ID' values and rename columns
filtered_rows_los = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209])).select(
    col('SUBMISSION_FISCAL_YEAR'),
    col('SITE_ID').alias('CORP_ID'),
    col('SITE_NAME').alias('CORP_NAME'),
    col('SITE_PEER').alias('CORP_PEER'),
    col('90th_Percentile_LOS').alias('PERCENTILE_90')
)

# Union the datasets using unionByName with allowMissingColumns=True
los_org_22 = los_org_22.unionByName(filtered_rows_los, allowMissingColumns=True)

# Formatting as string with two decimal places for consistency in display
los_org_22 = los_org_22.withColumn("PERCENTILE_90", F.format_number("PERCENTILE_90", 2))

# Show results and data schema
los_org_22.show(truncate=False)
print("Number of rows:", los_org_22.count())
print("Number of columns:", len(los_org_22.columns))
