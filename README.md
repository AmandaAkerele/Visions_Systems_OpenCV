from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import DoubleType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Assume los_org_all_yr_b is a DataFrame loaded previously
# Convert columns to numeric
los_org_all_yr_b = los_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast(DoubleType()))
los_org_all_yr_b = los_org_all_yr_b.withColumn("TIME", col("TIME").cast(DoubleType()))

# Drop rows with NaNs in specific columns
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=["PERCENTILE_90", "TIME"])

# Prepare data for linear regression
vectorAssembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
los_org_all_yr_b = vectorAssembler.transform(los_org_all_yr_b)

# Group data by 'CORP_ID' and fit model
grouped = los_org_all_yr_b.groupby("CORP_ID")
all_results = []

for key, group in grouped:
    lr = LinearRegression(featuresCol='features', labelCol='PERCENTILE_90')
    model = lr.fit(group)
    summary = model.summary
    
    all_results.append((key, 'PARAMS', model.coefficients[0], model.intercept))
    all_results.append((key, 'STDERR', summary.coefficientStandardErrors[0], summary.interceptStandardError))
    all_results.append((key, 'T', summary.tValues[0], summary.tValues[1]))
    all_results.append((key, 'PVALUE', summary.pValues[0], summary.pValues[1]))
    # Confidence intervals can be computed from standard errors and t-values if necessary

# Convert list of results into DataFrame
schema = ["CORP_ID", "_TYPE_", "TIME_COEFF", "INTERCEPT"]
results_df = spark.createDataFrame(all_results, schema=schema)

# Define improvement indicators based on P-values and coefficients
results_df = results_df.withColumn("linr", when((col("_TYPE_") == 'PVALUE') & (col("TIME_COEFF") < 0.05), 1).otherwise(0))
# More processing can be done here similar to the pandas merge and conditions

# The operations like merge and conditional updates need to be adapted to use Spark DataFrame functions like join and when



or 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType, ArrayType, DoubleType
from scipy.stats import linregress
import numpy as np

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

# Example of additional processing: Define improvement based on p-value and slope
results = results.withColumn(
    "IMPROVEMENT_IND_CODE",
    when((col("p_value") < 0.05) & (col("slope") > 0), lit("001"))
    .when((col("p_value") < 0.05) & (col("slope") < 0), lit("003"))
    .otherwise(lit("002"))
)

# Example of additional processing: Define descriptions
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

