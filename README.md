from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, when, lit, size, array
from pyspark.sql.types import FloatType, ArrayType
import numpy as np
from scipy.stats import linregress

# Initialize the Spark session
spark = SparkSession.builder.appName("Regression Analysis").getOrCreate()

# Assume los_org_all_yr_b is already loaded as a DataFrame
# Convert columns to numeric and drop NaNs
los_org_all_yr_b = los_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast("float"))
los_org_all_yr_b = los_org_all_yr_b.withColumn("TIME", col("TIME").cast("float"))
los_org_all_yr_b = los_org_all_yr_b.na.drop(subset=["PERCENTILE_90", "TIME"])

# Group by 'CORP_ID' and collect necessary columns for regression, ensure groups have enough points
grouped_data = los_org_all_yr_b.groupBy("CORP_ID").agg(
    collect_list("TIME").alias("times"),
    collect_list("PERCENTILE_90").alias("percentiles")
).filter(size("times") >= 3)  # Ensure each group has at least 3 points for stable regression

# Define a UDF for performing linear regression using scipy
def perform_regression(times, percentiles):
    if not times or not percentiles:
        return [None, None, None, None, None, None]  # Updated to include more diagnostic data
    try:
        slope, intercept, r_value, p_value, std_err = linregress(times, percentiles)
        return [float(slope), float(intercept), float(r_value), float(p_value), float(std_err), len(times)]
    except Exception as e:
        print(f"Error in regression: {str(e)}")
        return [None, None, None, None, None, None]

# Register the UDF
regression_udf = udf(perform_regression, ArrayType(FloatType()))

# Apply the UDF to compute regression parameters
results = grouped_data.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))

# Expand the results into separate columns and define improvement indicators
results = results.select(
    "CORP_ID",
    col("regression_results").getItem(0).alias("slope"),
    col("regression_results").getItem(1).alias("intercept"),
    col("regression_results").getItem(2).alias("r_value"),
    col("regression_results").getItem(3).alias("p_value"),
    col("regression_results").getItem(4).alias("std_err"),
    col("regression_results").getItem(5).alias("n_points"),  # Number of data points used
    when((col("p_value") < 0.05) & (col("slope") > 0), lit("001"))
        .when((col("p_value") < 0.05) & (col("slope") < 0), lit("003"))
        .otherwise(lit("002")).alias("IMPROVEMENT_IND_CODE"),
    when(col("IMPROVEMENT_IND_CODE") == "001", lit("Improving"))
        .when(col("IMPROVEMENT_IND_CODE") == "003", lit("Weakening"))
        .otherwise(lit("No Change")).alias("IMPROVEMENT_IND_E_DESC")
)

# Show final results with diagnostics
results.show(truncate=False)
