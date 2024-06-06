from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Detailed Percentile Calculation").getOrCreate()

def calculate_percentile(df, column, percentiles, bycols=None, confidence_interval=False, near_exact=False):
    # Ensure percentiles are expressed as floating point
    percentiles = [float(p) for p in percentiles]

    # Adjust relativeError based on whether near-exact calculation is required
    relative_error = 0.0001 if near_exact else 0.01  # Set to a very small number for near-exact calculations

    if bycols:
        # Calculate percentiles grouped by specified columns
        results = df.groupBy(bycols).agg(*[
            F.expr(f"percentile_approx({column}, array({','.join(map(str, percentiles))}), {relative_error})").alias(f"PERCENTILE_{int(p * 100)}")
            for p in percentiles
        ])
    else:
        # Calculate global percentiles for the entire dataframe
        global_percentiles = {f"PERCENTILE_{int(p * 100)}": df.approxQuantile(column, [p], relative_error)[0] for p in percentiles}
        results = spark.createDataFrame([global_percentiles])

    # Convert column names to upper case if needed
    return results.select([F.col(c).alias(c.upper()) for c in results.columns])

# Example usage of the function with near-exact calculation
tpia_nt_22 = calculate_percentile(TPIA_nt_record_ucc_22, "WAIT_TIME_TO_PIA_HOURS", [0, 0.5, 0.9, 0.999, 1], near_exact=True)
tpia_reg_22 = calculate_percentile(TPIA_nt_record_ucc_22, "WAIT_TIME_TO_PIA_HOURS", [0.9], bycols=["SUBMISSION_FISCAL_YEAR", 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'], near_exact=True)
