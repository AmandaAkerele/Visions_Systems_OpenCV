from pyspark.sql.window import Window
from pyspark.sql.functions import col, ntile
import pyspark.sql.functions as F

def calculate_percentile_spark(df, metric, percentiles, bycols=''):
    windowSpec = Window.partitionBy(bycols).orderBy(metric) if bycols else Window.orderBy(metric)
    final_df = None

    for percentile in percentiles:
        percentile_col_name = f'percentile_{percentile}'
        df_with_percentile = df.withColumn(percentile_col_name, ntile(int(100 * percentile)).over(windowSpec))

        # Getting the specific percentile value
        df_percentile = df_with_percentile.filter(col(percentile_col_name) == int(100 * percentile)).groupBy(bycols).agg(F.first(metric).alias(percentile_col_name))

        if final_df is None:
            final_df = df_percentile
        else:
            final_df = final_df.join(df_percentile, on=bycols, how='inner')

    return final_df
import numpy as np
import pandas as pd

def altman_confidence_interval(data, percentile):
    ct = len(data)
    if ct == 0:
        return np.nan, np.nan

    ci_low_n = round(ct * percentile - 1.96 * np.sqrt(ct * percentile * (1 - percentile))) - 1
    ci_low_n = max(min(ci_low_n, ct - 1), 0)
    ci_upp_n = round(1 + ct * percentile + 1.96 * np.sqrt(ct * percentile * (1 - percentile))) - 1
    ci_upp_n = max(min(ci_upp_n, ct - 1), 0)
    
    ci_low = np.nan if percentile in (1, 0) else data.iloc[ci_low_n]
    ci_upp = np.nan if percentile in (1, 0) else data.iloc[ci_upp_n]

    return ci_low, ci_upp

# Example Usage (Assuming df is your PySpark DataFrame and 'metric' is the column you're analyzing)
percentile = 0.5  # Example percentile
bycols = 'your_grouping_column'  # Replace with your actual grouping column

# Calculate Percentiles in PySpark
percentile_df = calculate_percentile_spark(df, 'metric', [percentile], bycols)

# Collect Data to Driver for Confidence Interval Calculation
collected_data = percentile_df.toPandas()

# Apply the Confidence Interval UDF
collected_data[['ci_lower', 'ci_upper']] = collected_data.apply(lambda row: altman_confidence_interval(row['metric'], percentile), axis=1, result_type='expand')
