from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark session if not already done
spark = SparkSession.builder.appName("Accurate Percentile Calculation").getOrCreate()

# Assuming 'ed_record_admit_with_ucc_22' is a DataFrame loaded in Spark
# Example data loading (commented for context)
# ed_record_admit_with_ucc_22 = spark.read.csv("data.csv", header=True, inferSchema=True)

# Calculate the 90th percentile directly using percentile_approx
los_reg = ed_record_admit_with_ucc_22.groupBy("SUBMISSION_FISCAL_YEAR", "NEW_REGION_ID", "REGION_E_DESC") \
    .agg(expr("percentile_approx(LOS_HOURS, 0.9)").alias("90th_Percentile_LOS"))

# Show initial results to check before rounding
los_reg.show()

# Apply rounding if necessary - Round down to one decimal place
from pyspark.sql.functions import round
los_reg = los_reg.withColumn("90th_Percentile_LOS", round("90th_Percentile_LOS", 1))

# Show the final adjusted results
los_reg.show()

# Convert to Pandas DataFrame for better display (optional)
pandas_df = los_reg.toPandas()
display(pandas_df)
