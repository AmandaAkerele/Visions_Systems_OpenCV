from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assuming 'spark' is your SparkSession
# spark = SparkSession.builder.appName("Detailed Percentile Calculation").getOrCreate()

# Assuming 'ed_record_admit' is already a DataFrame loaded into PySpark
# ed_record_admit = spark.read.csv("data.csv", header=True, inferSchema=True)

# Define a window partitioned by the necessary groups and ordered by LOS_HOURS
windowSpec = Window.partitionBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").orderBy("LOS_HOURS")

# Add a row number over the window
ranked = ed_record_admit.withColumn("rank", F.rank().over(windowSpec))

# Calculate the total number of entries per group
total_counts = ranked.groupBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER").agg(F.max("rank").alias("total"))

# Join back on the original DataFrame to filter to approximately the 90th percentile
cond = [ranked.SUBMISSION_FISCAL_YEAR == total_counts.SUBMISSION_FISCAL_YEAR, 
        ranked.SITE_ID == total_counts.SITE_ID, 
        ranked.SITE_NAME == total_counts.SITE_NAME, 
        ranked.SITE_PEER == total_counts.SITE_PEER]

ninety_pct = ranked.join(total_counts, cond)\
    .filter((ranked.rank / total_counts.total) >= 0.9)\
    .groupBy("SUBMISSION_FISCAL_YEAR", "SITE_ID", "SITE_NAME", "SITE_PEER")\
    .agg(F.min("LOS_HOURS").alias("90th_Percentile_LOS"))

# Show the result
ninety_pct.show()

# Optionally convert to Pandas DataFrame for display purposes
pandas_df = ninety_pct.toPandas()
display(pandas_df)
