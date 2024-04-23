from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, when, lit, array
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField
from scipy.stats import linregress
import numpy as np

spark = SparkSession.builder.appName("Regression Analysis").getOrCreate()

# Assume los_org_all_yr_b and ed_nacrs_flg_1 are already loaded as DataFrames
# Conversion of column types and dropping NaNs are handled as per the provided code

# Define a UDF for performing linear regression using scipy
def perform_regression(times, percentiles):
    if not times or not percentiles:
        return [None, None, None, None]
    slope, intercept, r_value, p_value, std_err = linregress(times, percentiles)
    return [float(slope), float(intercept), float(p_value), float(std_err)]

schema = ArrayType(FloatType())
regression_udf = udf(perform_regression, schema)

# Apply the UDF to compute regression parameters
grouped_data = los_org_all_yr_b.groupBy("CORP_ID").agg(
    collect_list(col("TIME")).alias("times"),
    collect_list(col("PERCENTILE_90")).alias("percentiles")
)
results = grouped_data.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))

# Expand the results into separate columns and define improvement indicators
results = results.select(
    "CORP_ID",
    col("regression_results").getItem(0).alias("slope"),
    col("regression_results").getItem(1).alias("intercept"),
    col("regression_results").getItem(2).alias("p_value"),
    col("regression_results").getItem(3).alias("std_err"),
    when((col("p_value") < 0.05) & (col("slope") > 0), lit("001"))
        .when((col("p_value") < 0.05) & (col("slope") < 0), lit("003"))
        .otherwise(lit("002")).alias("IMPROVEMENT_IND_CODE"),
    when(col("IMPROVEMENT_IND_CODE") == "001", lit("Improving"))
        .when(col("IMPROVEMENT_IND_CODE") == "003", lit("Weakening"))
        .otherwise(lit("No Change")).alias("IMPROVEMENT_IND_E_DESC")
)

# Assuming ed_nacrs_flg_1_SL is already loaded and corresponds to ed_nacrs_flg_1 in your pandas script
los_reg_22_ta = results.join(
    ed_nacrs_flg_1_SL,
    results['CORP_ID'] == ed_nacrs_flg_1_SL['CORP_ID'],
    'left_anti'
)

# Show and save final results (optional saving commented out)
results.show()
# results.write.format("parquet").save("/path/to/output")



or 


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, when, lit, array
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField
import numpy as np
from scipy.stats import linregress

spark = SparkSession.builder.appName("Regression Analysis").getOrCreate()

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
    if not times or not percentiles:
        return [None, None, None, None]
    slope, intercept, r_value, p_value, std_err = linregress(times, percentiles)
    return [float(slope), float(intercept), float(p_value), float(std_err)]

regression_udf = udf(perform_regression, ArrayType(FloatType()))

# Apply the UDF to compute regression parameters
results = grouped_data.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))

# Expand the results into separate columns and define improvement indicators
results = results.select(
    "CORP_ID",
    col("regression_results").getItem(0).alias("slope"),
    col("regression_results").getItem(1).alias("intercept"),
    col("regression_results").getItem(2).alias("p_value"),
    col("regression_results").getItem(3).alias("std_err"),
    when((col("p_value") < 0.05) & (col("slope") > 0), lit("001"))
        .when((col("p_value") < 0.05) & (col("slope") < 0), lit("003"))
        .otherwise(lit("002")).alias("IMPROVEMENT_IND_CODE"),
    when(col("IMPROVEMENT_IND_CODE") == "001", lit("Improving"))
        .when(col("IMPROVEMENT_IND_CODE") == "003", lit("Weakening"))
        .otherwise(lit("No Change")).alias("IMPROVEMENT_IND_E_DESC")
)

# Show final results
results.show()


or

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, when, lit, array
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField
from scipy.stats import linregress
import numpy as np

# Initialize the Spark session
spark = SparkSession.builder.appName("Advanced Analytics").getOrCreate()

# Assume tpia_org_all_yr_b is already loaded as a DataFrame
# Convert columns to numeric and drop NaNs
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("PERCENTILE_90", col("PERCENTILE_90").cast("float"))
tpia_org_all_yr_b = tpia_org_all_yr_b.withColumn("TIME", col("TIME").cast("float"))
tpia_org_all_yr_b = tpia_org_all_yr_b.na.drop(subset=["PERCENTILE_90", "TIME"])

# Group by 'CORP_ID' and collect necessary columns for regression
grouped_data = tpia_org_all_yr_b.groupBy("CORP_ID").agg(
    collect_list("TIME").alias("times"),
    collect_list("PERCENTILE_90").alias("percentiles")
)

# Define a UDF for performing linear regression using scipy
def perform_regression(times, percentiles):
    slope, intercept, r_value, p_value, std_err = linregress(times, percentiles)
    return [float(slope), float(intercept), float(p_value), float(std_err)]

# Register the UDF
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

# Assuming tpia_org_p_val_parms is another DataFrame that filters or modifies this results DataFrame,
# apply whatever transformations or filters are needed here. 
# For example, if filtering out certain 'CORP_ID's that exist in another DataFrame ed_nacrs_flg_1_SL:
if 'ed_nacrs_flg_1_SL' in locals() or 'ed_nacrs_flg_1_SL' in globals():
    tpia_org_p_val_parms = results.join(ed_nacrs_flg_1_SL, results["CORP_ID"] == ed_nacrs_flg_1_SL["CORP_ID"], "left_anti")

# Show final results
results.show()
