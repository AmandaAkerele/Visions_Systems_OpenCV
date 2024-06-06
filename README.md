from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Ensure SparkSession is initialized
spark = SparkSession.builder.appName("Detailed Percentile Calculation").getOrCreate()

def calculate_percentile(df, column, percentiles, bycols=None, confidence_interval=False, exact=False):
    # Ensure percentiles are expressed as floating point
    percentiles = [float(p) for p in percentiles]

    # Adjust relativeError based on whether exact calculation is required
    # Change to a very small value that is not zero
    relative_error = 0.00001 if exact else 0.01  # Use a very small number for high precision that is non-zero

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

# Example usage of the function with high precision but not exact
tpia_nt_22 = calculate_percentile(TPIA_nt_record_ucc_df, "WAIT_TIME_TO_PIA_HOURS", [0, 0.5, 0.9, 0.999, 1], exact=True)
tpia_reg_22 = calculate_percentile(TPIA_nt_record_ucc_df, "WAIT_TIME_TO_PIA_HOURS", [0.9], bycols=["SUBMISSION_FISCAL_YEAR", 'adm_prov_plldj_name_e_desc', 'adm_region_id', 'adm_region_pll_name_e_desc'], exact=True)
