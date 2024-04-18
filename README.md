from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

# Join and filter, ensuring aliases are used to avoid ambiguity
los_org_22 = ranked.alias("r").join(
    total_counts.alias("t"),
    (col("r.SUBMISSION_FISCAL_YEAR") == col("t.SUBMISSION_FISCAL_YEAR")) &
    (col("r.CORP_ID") == col("t.CORP_ID")) &
    (col("r.CORP_NAME") == col("t.CORP_NAME")) &
    (col("r.CORP_PEER") == col("t.CORP_PEER"))
).filter(col("r.rank") == col("t.ninety_pct_rank"))

# Group and aggregate, using clear column references
los_org_22 = los_org_22.groupBy(col("r.SUBMISSION_FISCAL_YEAR"), col("r.CORP_ID"), col("r.CORP_NAME"), col("r.CORP_PEER")).agg(
    F.min(col("r.LOS_HOURS")).alias("PERCENTILE_90")
)

# Further operations as before
filtered_rows_los = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209])).select(
    col('SUBMISSION_FISCAL_YEAR'),
    col('SITE_ID').alias('CORP_ID'),
    col('SITE_NAME').alias('CORP_NAME'),
    col('SITE_PEER').alias('CORP_PEER'),
    col('90th_Percentile_LOS').alias('PERCENTILE_90')
)

# Union and display
los_org_22 = los_org_22.unionByName(filtered_rows_los, allowMissingColumns=True)
los_org_22.show(truncate=False)
