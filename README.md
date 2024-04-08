import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField

# Define the percentile_ci function using PySpark DataFrame operations
def percentile_ci(df, percentile, confidence_interval):
    df = df.dropna().sort(F.col(df.columns[0])).toPandas()
    ct = df.shape[0]

    if ct > 0:
        kf = (ct - 1) * percentile
        pt_low_n = int(kf.floor()) + 1 - 1
        pt_low_n = max(0, pt_low_n)
        pt_upp_n = int(kf.floor()) + 2 - 1
        pt_upp_n = min(ct - 1, pt_upp_n)
        pt_upp_n = max(0, pt_upp_n)
        
        d = kf - kf.floor()
        
        point_est = df.iloc[pt_low_n] - df.iloc[pt_low_n] * d
        point_est = point_est if pd.isna(df.iloc[pt_upp_n]) else df.iloc[pt_low_n] + d * (df.iloc[pt_upp_n] - df.iloc[pt_low_n])
        point_est = round(point_est * 10000) / 10000

        # Altman CI
        ci_low_n = ct * percentile - 1.96 * (ct * percentile * (1 - percentile)) ** 0.5 - 1
        ci_low_n = max(0, ci_low_n)
        
        ci_upp_n = 1 + ct * percentile + 1.96 * (ct * percentile * (1 - percentile)) ** 0.5 - 1
        ci_upp_n = min(ct - 1, ci_upp_n)
        
        ci_low = df.iloc[int(ci_low_n)] if (percentile == 1 or percentile == 0) else ci_low_n
        ci_upp = df.iloc[int(ci_upp_n)] if (percentile == 1 or percentile == 0) else ci_upp_n
        
    else:
        point_est = None
        ci_low = None
        ci_upp = None

    if confidence_interval:
        return ci_low, ci_upp
    else:
        return point_est

# Define the calculate_percentile function using PySpark DataFrame operations
def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=''):
    strg = [str(round(100 * x)) if 100 * x == round(100 * x) else str(100 * x).replace('.', '_') for x in ppt]
    
    for i in range(len(ppt)):
        if bycols == '':
            df = df.withColumn('__', F.lit(1))  # Add a constant column '__' with value 1
            bycols = '__'
        
        y = df.groupby(bycols).apply(lambda x: percentile_ci(x.select(metric), ppt[i], confidence_interval))
        
        out = y.tolist()
        index = y.index.tolist()
        out_df = ps.DataFrame(out, index=index).reset_index()
        out_df.columns = [str(col) for col in out_df.columns]
        out_df = out_df.rename(columns={'0': 'percentile_' + strg[i] + '_ci_lower',
                                        '1': 'percentile_' + strg[i] + '_ci_upper'})
        if i == 0:
            final = out_df
        else:
            final = final.merge(out_df, on=bycols)

    if bycols == '__':
        final.drop(columns=['__'], inplace=True)

    return final

# Convert pandas DataFrame to PySpark DataFrame
ed_record_admit_with_ucc_22_spark = ps.DataFrame(ed_record_admit_with_ucc_22)

# Call the calculate_percentile function
los_regg = calculate_percentile(ed_record_admit_with_ucc_22_spark, 'LOS_HOURS', [0.9], confidence_interval=True)
los_regg.show()



or 
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# Define the calculate_percentile function using PySpark's window functions
def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=''):
    strg = [str(round(100 * x)) if 100 * x == round(100 * x) else str(100 * x).replace('.', '_') for x in ppt]
    
    # Define window specification
    windowSpec = Window.partitionBy(bycols).orderBy(metric)
    
    for i in range(len(ppt)):
        # Compute point estimate
        percentile_val = ppt[i]
        df = df.withColumn("rank", F.percent_rank().over(windowSpec))
        df = df.withColumn("point_estimate", F.first(metric).over(windowSpec).alias(f'point_estimate_{strg[i]}'))
        
        if confidence_interval:
            # Compute confidence interval
            df = df.withColumn("ci_low", F.when((F.col("rank") < percentile_val), F.first(metric).over(windowSpec)).otherwise(None))
            df = df.withColumn("ci_high", F.when((F.col("rank") >= percentile_val), F.first(metric).over(windowSpec)).otherwise(None))
            
        if i == 0:
            final = df.select(bycols, f'point_estimate_{strg[i]}', 'ci_low', 'ci_high')
        else:
            new_cols = [f'point_estimate_{strg[i]}', f'ci_low', f'ci_high']
            final = final.join(df.select(bycols + new_cols), on=bycols, how='inner')

    return final

# Convert pandas DataFrame to PySpark DataFrame
ed_record_admit_with_ucc_22_spark = ps.DataFrame(ed_record_admit_with_ucc_22)

# Call the calculate_percentile function
los_regg = calculate_percentile(ed_record_admit_with_ucc_22_spark, 'LOS_HOURS', [0.9], confidence_interval=True)
los_regg.show()



