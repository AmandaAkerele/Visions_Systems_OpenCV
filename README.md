from pyspark.sql.functions import col, count, when

# Function to process percentile data for different IDs
def process_percentile_data(df, id_column):
    # Directly compute the number of years each ID was in the PERCENTILE_90
    count_df = df.withColumn(
        "in_percentile_90",
        when(col("PERCENTILE_90").isNotNull(), 1).otherwise(0)
    ).groupBy(id_column).agg(
        count(when(col("in_percentile_90") == 1, True)).alias("years_in_percentile_90")
    )

    # Filter out IDs that have been in PERCENTILE_90 for at least 3 years
    filtered_df = count_df.filter(col("years_in_percentile_90") >= 3)

    # Join and select records for the last year
    comp_df = df.join(
        filtered_df, id_column, "inner"
    ).filter(col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))

    # Similarly, select records for the last three years
    trend_df = df.join(
        filtered_df, id_column, "inner"
    ).filter(col("SUBMISSION_FISCAL_YEAR").isin([str(closed_year-1), str(closed_year-2), str(closed_year-3)]))

    return comp_df, trend_df

# Apply the function to the corporate data
corp_comp, corp_trend = process_percentile_data(tpia_org_combined, "CORP_ID")

# Apply the function to the regional data
reg_comp, reg_trend = process_percentile_data(tpia_reg_combined, "adm_region_id")

# Optionally display results
# corp_comp.show()
# corp_trend.show()
# reg_comp.show()
# reg_trend.show()
