from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, ArrayType
import numpy as np

# Define the output schema of your UDF
schema = StructType([
    StructField("percentile", DoubleType(), False),
    StructField("ci_lower", DoubleType(), False),
    StructField("ci_upper", DoubleType(), False)
])

def percentile_ci_udf(values, percentile, confidence_interval):
    values = sorted([v for v in values if v is not None])
    n = len(values)
    if n == 0:
        return (None, None, None)
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

# Register UDF
percentile_udf = udf(percentile_ci_udf, schema)

# Usage Example
df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ['values'])
df = df.select(percentile_udf(F.collect_list('values').over(Window.partitionBy()), F.lit(0.5), F.lit(True)).alias('result'))
df.select(
    F.col('result.percentile').alias('Percentile'),
    F.col('result.ci_lower').alias('CI Lower'),
    F.col('result.ci_upper').alias('CI Upper')
).show()
