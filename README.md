import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Verify column names
print(ed_record_admit_with_ucc_22.columns)

# Filter DataFrame using columns mentioned in the error
filtered_df = ed_record_admit_with_ucc_22.select("GENDER", "AGE_NUM", "CORP_ID", "SITE_ID", "CORP_NAME")

# Show first few rows of filtered DataFrame
filtered_df.show()

# Define window specification
windowSpec = Window.orderBy("LOS_HOURS")

# Define the calculate_percentile function using PySpark DataFrame operations
def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=''):
    strg = [str(round(100 * x)) if 100 * x == round(100 * x) else str(100 * x).replace('.', '_') for x in ppt]
    for i in range(len(ppt)):
        if bycols == '':
            df = df.withColumn('__', F.lit(1))  # Add a constant column '__' with value 1
        
        df = df.withColumn("rank", F.percent_rank().over(windowSpec))
        df = df.withColumn("point_estimate", F.first(metric).over(windowSpec).alias(f'point_estimate_{strg[i]}'))
        
        # Here, integrate your percentile_ci function or computation logic
        # df = percentile_ci(df, metric, ppt[i], confidence_interval)
        
    if bycols == '__':
        df = df.drop('__')

    return df

# Call the calculate_percentile function
los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
los_regg.show()
