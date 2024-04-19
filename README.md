from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField

# Initialize Spark session
spark = SparkSession.builder.appName("Advanced Percentile Calculation").getOrCreate()

# Assuming data is loaded into DataFrame `los_nt_record_ucc_22`, e.g., from a CSV, Parquet, etc.
# For example, if your data is in a CSV:
# los_nt_record_ucc_22 = spark.read.csv("path_to_los_nt_record_ucc_22.csv", header=True, inferSchema=True)

# Define the schema for the UDAF's output
schema = StructType([
    StructField("percentile", DoubleType()),
    StructField("ci_lower", DoubleType()),
    StructField("ci_upper", DoubleType())
])

@F.udf(schema)
def percentile_ci_udf(values, percentile, confidence_interval):
    import numpy as np
    values = sorted([v for v in values if v is not None])  # Remove None and sort
    n = len(values)
    if n == 0:
        return None
    k = int((n - 1) * percentile)
    f = (n - 1) * percentile - k
    percentile_value = values[k] + (values[min(k+1, n-1)] - values[k]) * f
    result = [round(percentile_value, 4)]
    
    if confidence_interval:
        se = np.sqrt(percentile * (1 - percentile) / n)
        z_score = 1.96  # for 95% confidence
        ci_lower = percentile_value - z_score * se
        ci_upper = percentile_value + z_score * se
        result.extend([ci_lower, ci_upper])
    
    return tuple(result)

def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=None):
    col_list = [F.col(c) for c in bycols] if bycols else []
    results = []

    for p in ppt:
        agg_col = F.collect_list(metric).alias('data')
        df_agg = df.groupBy(*col_list).agg(agg_col)
        result_col = percentile_ci_udf(F.col('data'), F.lit(p), F.lit(confidence_interval))
        df_result = df_agg.select(*col_list, result_col.alias('result'))
        
        df_flattened = df_result.select(
            *col_list,
            F.col('result.percentile').alias(f'percentile_{int(p*100)}'),
            F.col('result.ci_lower').alias(f'percentile_{int(p*100)}_ci_lower'),
            F.col('result.ci_upper').alias(f'percentile_{int(p*100)}_ci_upper')
        )
        results.append(df_flattened)
    
    final_df = results[0]
    for df in results[1:]:
        final_df = final_df.join(df, on=bycols, how='inner')
    
    return final_df

# Calculate percentiles for los_nt_record_ucc_22
los_nt_22 = calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS', [0,0.5,0.9,0.999,1], True)
los_nt_22 = los_nt_22.select([F.col(c).alias(c.upper()) for c in los_nt_22.columns])  # Rename columns to uppercase

# Calculate grouped percentiles for los_nt_record_ucc_22
los_reg_22 = calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], True, ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])
los_reg_22 = los_reg_22.select([F.col(c).alias(c.upper()) for c in los_reg_22.columns])  # Rename columns to uppercase

# Show results
los_nt_22.show()
los_reg_22.show()
