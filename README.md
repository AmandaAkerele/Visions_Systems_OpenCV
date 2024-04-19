from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Advanced Percentile Calculation").getOrCreate()

# Define the schema for the UDAF's output
schema = StructType([
    StructField("percentile", DoubleType()),
    StructField("ci_lower", DoubleType()),
    StructField("ci_upper", DoubleType())
])

@F.udf(schema)
def percentile_ci_udf(values, percentile, confidence_interval):
    import numpy as np
    values = sorted([v for v in values if v is not None])  # sorting and removing None values
    n = len(values)
    if n == 0:
        return None
    k = int((n - 1) * percentile)
    lower_index = max(min(k, n - 1), 0)
    upper_index = max(min(k + 1, n - 1), 0)
    percentile_value = values[lower_index] + (values[upper_index] - values[lower_index]) * (n*percentile - k)
    result = [round(percentile_value, 4)]
    
    if confidence_interval:
        se = np.sqrt(percentile * (1 - percentile) / n)
        z_score = 1.96  # for 95% confidence
        ci_lower = percentile_value - z_score * se
        ci_upper = percentile_value + z_score * se
        result.extend([ci_lower, ci_upper])
    
    return tuple(result)

def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=None):
    # Prepare the DataFrame for processing
    col_list = [F.col(c) for c in bycols] if bycols else []
    results = []

    for p in ppt:
        # Collect data into a list, calculate percentile and CI
        agg_col = F.collect_list(metric).alias('data')
        df_agg = df.groupBy(*col_list).agg(agg_col)
        result_col = percentile_ci_udf(F.col('data'), F.lit(p), F.lit(confidence_interval))
        df_result = df_agg.select(*col_list, result_col.alias('result'))
        
        # Flatten the structure for easy use
        df_flattened = df_result.select(
            *col_list,
            F.col('result.percentile').alias(f'percentile_{int(p*100)}'),
            F.col('result.ci_lower').alias(f'percentile_{int(p*100)}_ci_lower'),
            F.col('result.ci_upper').alias(f'percentile_{int(p*100)}_ci_upper')
        )
        results.append(df_flattened)
    
    # Merge all percentile results into one DataFrame
    final_df = results[0]
    for df in results[1:]:
        final_df = final_df.join(df, on=bycols, how='inner')
    
    return final_df


based on the code above. 

covert the code below to pyspark to read the percentile 

los_nt_22=calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS', [0,0.5,0.9,0.999,1],confidence_interval=True)
los_nt_22=los_nt_22.rename(columns=lambda x: x.upper())

los_reg_22=calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','FACILITY_PROVINCE','NEW_REGION_ID','REGION_E_DESC'])
los_reg_22=los_reg_22.rename(columns=lambda x: x.upper())
