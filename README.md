from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, when, lit
from pyspark.sql.types import FloatType, ArrayType
from scipy.stats import linregress

# Initialize Spark Session
spark = SparkSession.builder.appName("advanced_example").getOrCreate()

# Assume los_org_all_yr_b is already loaded as a DataFrame
# Convert columns to numeric and drop NaNs
los_org_all_yr_b = los_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast("float"))
los_org_all_yr_b = los_org_all_yr_b.withColumn("TIME", col("TIME").cast("float"))
los_org_all_yr_b = los_org_all_yr_b.na.drop(subset=["PERCENTILE_90", "TIME"])

# Group by 'CORP_ID' and collect necessary columns for regression
grouped_data = los_org_all_yr_b.groupBy("CORP_ID").agg(
    collect_list("TIME").alias("times"),
    collect_list("PERCENTILE_90").alias("percentiles")
)

# Define a UDF for performing linear regression using scipy
def perform_regression(times, percentiles):
    slope, intercept, r_value, p_value, std_err = linregress(times, percentiles)
    return [float(slope), float(intercept), float(p_value), float(std_err)]

regression_udf = udf(perform_regression, ArrayType(FloatType()))

# Apply the UDF to compute regression parameters
results = grouped_data.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))

# Expand the results into separate columns
results = results.select(
    "CORP_ID",
    results.regression_results[0].alias("slope"),
    results.regression_results[1].alias("intercept"),
    results.regression_results[2].alias("p_value"),
    results.regression_results[3].alias("std_err")
)

# Define improvement indicators based on p-value and slope
results = results.withColumn(
    "IMPROVEMENT_IND_CODE",
    when((col("p_value") < 0.05) & (col("slope") > 0), lit("001"))
    .when((col("p_value") < 0.05) & (col("slope") < 0), lit("003"))
    .otherwise(lit("002"))
)

# Define descriptions
results = results.withColumn(
    "IMPROVEMENT_IND_E_DESC",
    when(col("IMPROVEMENT_IND_CODE") == "001", lit("Improving"))
    .when(col("IMPROVEMENT_IND_CODE") == "003", lit("Weakening"))
    .otherwise(lit("No Change"))
)

# Show final results
results.show()

# Assuming you might also want to save or further process the results
# results.write.format("parquet").save("/path/to/output")
