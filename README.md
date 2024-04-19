import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql import Window
import numpy as np

# Define the UDF
def calculate_percentile_ci(values_list, percentile, ci):
    values_list = [x for x in values_list if x is not None]
    values_list.sort()
    n = len(values_list)
    if n == 0:
        return float('nan'), float('nan'), float('nan')
    
    # Calculate percentile
    k = (n-1) * percentile
    f = np.floor(k)
    c = np.ceil(k)
    if f == c:
        percentile_value = values_list[int(k)]
    else:
        d0 = values_list[int(f)] * (c-k)
        d1 = values_list[int(c)] * (k-f)
        percentile_value = d0 + d1
    
    # Calculate confidence interval if needed
    if ci:
        z = 1.96  # 95% CI, hardcoded
        se = np.sqrt(percentile * (1 - percentile) / n)
        ci_lower = percentile_value - z * se
        ci_upper = percentile_value + z * se
        return round(percentile_value, 4), round(ci_lower, 4), round(ci_upper, 4)
    else:
        return round(percentile_value, 4), None, None

# Register UDF
schema = StructType([
    StructField("percentile", DoubleType(), False),
    StructField("ci_lower", DoubleType(), True),
    StructField("ci_upper", DoubleType(), True)
])
percentile_ci_udf = F.udf(calculate_percentile_ci, schema)


from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("Advanced Percentile Calculation").getOrCreate()

# Sample DataFrame loading
# Assuming df is already loaded e.g., df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Example DataFrame operation
def calculate_percentiles(df, column_name, percentiles, ci, group_cols=None):
    # Collect values into a list per group
    if group_cols:
        grouped_data = df.groupBy(group_cols).agg(F.collect_list(column_name).alias("values"))
    else:
        grouped_data = df.agg(F.collect_list(column_name).alias("values"))
    
    # Calculate percentile and CI for each percentile
    results = []
    for p in percentiles:
        result = grouped_data.withColumn("result", percentile_ci_udf(F.col("values"), F.lit(p), F.lit(ci)))
        result = result.select(group_cols + [F.col("result.percentile").alias(f"percentile_{int(p*100)}"),
                                             F.col("result.ci_lower").alias(f"ci_lower_{int(p*100)}"),
                                             F.col("result.ci_upper").alias(f"ci_upper_{int(p*100)}")])
        results.append(result)
    
    # Merge results
    from functools import reduce
    final_result = reduce(lambda x, y: x.join(y, group_cols), results)
    return final_result

# Applying the function
percentiles = [0, 0.5, 0.9, 0.999, 1]
group_cols = ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC']
df_result = calculate_percentiles(df, "LOS_HOURS", percentiles, True, group_cols)
df_result.show()

