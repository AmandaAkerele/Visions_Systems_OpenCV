from pyspark.sql import SparkSession
from pyspark.sql.functions as F

spark = SparkSession.builder.appName("Healthcare Analysis").getOrCreate()

def calculate_percentile(df, column, percentiles, bycols=None, confidence_interval=False):
    # Ensure percentiles are expressed in floating point
    percentiles = [float(p) for p in percentiles]

    if bycols:
        results = df.groupBy(bycols).agg(*[
            F.expr(f'percentile_approx({column}, {p})').alias(f'PERCENTILE_{int(p * 100)}') for p in percentiles
        ])
    else:
        # Calculating global percentiles for the entire dataframe
        global_percentiles = [df.approxQuantile(column, [p], 0.05)[0] for p in percentiles]
        percentile_cols = {f'PERCENTILE_{int(p * 100)}': val for p, val in zip(percentiles, global_percentiles)}
        results = spark.createDataFrame([percentile_cols])

    if confidence_interval:
        # Additional logic for confidence intervals if needed
        pass
    
    # Convert column names to upper case
    return results.select([F.col(c).alias(c.upper()) for c in results.columns])

# Example usage of the function
tpia_nt_22 = calculate_percentile(TPIA_nt_record_ucc_df, 'WAIT_TIME_TO_PIA_HOURS', [0, 0.5, 0.9, 0.999, 1])
