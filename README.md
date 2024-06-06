from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.appName("Healthcare Analysis").getOrCreate()


def calculate_percentile(df, column, percentiles, bycols=None, confidence_interval=False):
    # If bycols is specified, perform groupBy operation, else calculate on whole DataFrame
    if bycols:
        # Group by the specified columns and calculate percentiles within each group
        results = df.groupBy(bycols).agg(*[
            F.expr(f'percentile_approx({column}, {p})').alias(f'PERCENTILE_{int(p * 100)}') for p in percentiles
        ])
    else:
        # Calculate global percentiles without grouping
        global_percentiles = {f'PERCENTILE_{int(p * 100)}': df.approxQuantile(column, [p], 0.05)[0] for p in percentiles}
        results = spark.createDataFrame([global_percentiles])

    # Convert column names to upper case if required
    if confidence_interval:
        # Handle confidence interval logic if needed (more complex, often requires additional stats)
        pass
    return results.select([col(c).alias(c.upper()) for c in results.columns])


# Calculating percentiles for different categorizations
tpia_nt_22 = calculate_percentile(TPIA_nt_record_ucc_df, 'WAIT_TIME_TO_PIA_HOURS', [0, 0.5, 0.9, 0.999, 1], confidence_interval=True)
tpia_reg_22 = calculate_percentile(TPIA_nt_record_ucc_df, 'WAIT_TIME_TO_PIA_HOURS', [0.9], bycols=['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])
tpia_prov_22 = calculate_percentile(TPIA_nt_record_ucc_df, 'WAIT_TIME_TO_PIA_HOURS', [0.9], bycols=['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'PROVINCE_ID', 'PROVINCE_NAME'])
tpia_peer_22 = calculate_percentile(ed_record_admit_22_Peer, 'WAIT_TIME_TO_PIA_HOURS', [0.9], bycols=['SUBMISSION_FISCAL_YEAR', 'SITE_PEER'])
tpia_org_22 = calculate_percentile(ed_record, 'WAIT_TIME_TO_PIA_HOURS', [0.9], bycols=['SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_NAME', 'CORP_PEER'])
tpia_site_22 = calculate_percentile(ed_record, 'WAIT_TIME_TO_PIA_HOURS', [0.9], bycols=['SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER'])

# Filtering and renaming for Huron Perth
TPIA_site_Huron_Perth = tpia_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))

# Filter tpia_site DataFrame based on the condition, select required columns, and rename
filtered_rows = tpia_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))
filtered_rows = filtered_rows.select('SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER', col('PERCENTILE_90').alias('CORP_PEER'))

# Concatenating DataFrames in PySpark, requires creating a union
tpia_org_22 = tpia_org_22.unionByName(filtered_rows)
