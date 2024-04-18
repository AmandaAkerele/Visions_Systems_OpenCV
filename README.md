from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define a window partitioned by the necessary groups and ordered by LOS_HOURS
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").orderBy("LOS_HOURS")

# Add a row number over the window
ranked = ed_record_admit_with_ucc_22.withColumn("rank", F.rank().over(windowSpec))

# Calculate the total number of entries per group
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC").agg(F.max("rank").alias("total"))

# Properly alias all columns in the total_counts DataFrame to avoid ambiguity
total_counts = total_counts.select(
    F.col("SUBMISSION_FISCAL_YEAR").alias("total_SUBMISSION_FISCAL_YEAR"),
    F.col("NEW_REGION_ID").alias("total_NEW_REGION_ID"),
    F.col("REGION_E_DESC").alias("total_REGION_E_DESC"),
    "total"
)

# Join back on the original DataFrame to filter to approximately the 90th percentile
cond = [
    ranked.SUBMISSION_FISCAL_YEAR == total_counts.total_SUBMISSION_FISCAL_YEAR, 
    ranked.NEW_REGION_ID == total_counts.total_NEW_REGION_ID, 
    ranked.REGION_E_DESC == total_counts.total_REGION_E_DESC
]

ninety_pct = ranked.join(total_counts, cond)\
    .filter((ranked.rank / total_counts.total) >= 0.9)\
    .groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC")\
    .agg(F.min("LOS_HOURS").alias("90th_Percentile_LOS"))

# Show the result
ninety_pct.show()

# Optionally convert to Pandas DataFrame for display purposes
pandas_df = ninety_pct.toPandas()
display(pandas_df)
