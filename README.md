from pyspark.sql import functions as F
from pyspark.sql.window import Window

def percentile_ci(df, metric, percentile, confidence_interval=False):
    windowSpec = Window.orderBy(metric)
    
    df = df.filter(F.col(metric).isNotNull())
    df = df.withColumn("rank", F.percent_rank().over(windowSpec))
    
    count = df.count()
    
    if count > 0:
        kf = (count - 1) * percentile
        pt_low_n = F.floor(F.lit(kf)) + 1 - 1
        pt_low_n = F.when(pt_low_n < 0, 0).otherwise(pt_low_n)
        pt_upp_n = F.floor(F.lit(kf)) + 2 - 1
        pt_upp_n = F.when(pt_upp_n > count - 1, count - 1).otherwise(pt_upp_n)
        
        d = kf - F.floor(F.lit(kf))
        
        point_est = (F.when(F.isnull(df.select(metric).collect()[pt_upp_n][metric]), 
                            df.select(metric).collect()[pt_low_n][metric] - 
                            df.select(metric).collect()[pt_low_n][metric] * d)
                     .otherwise(df.select(metric).collect()[pt_low_n][metric] + 
                                d * (df.select(metric).collect()[pt_upp_n][metric] - 
                                     df.select(metric).collect()[pt_low_n][metric])))
        
        point_est = (F.round(point_est * 10000) / 10000).alias(f'point_estimate_{percentile}')
        
        # Altman CI
        ci_low_n = F.round(count * percentile - 1.96 * (count * percentile * (1 - percentile)) ** 0.5) - 1
        ci_low_n = F.when(ci_low_n < 0, 0).otherwise(ci_low_n)
        ci_upp_n = F.round(1 + count * percentile + 1.96 * (count * percentile * (1 - percentile)) ** 0.5) - 1
        ci_upp_n = F.when(ci_upp_n > count - 1, count - 1).otherwise(ci_upp_n)
        
        ci_low = F.when((percentile == 1) | (percentile == 0), None).otherwise(df.select(metric).collect()[ci_low_n][metric])
        ci_upp = F.when((percentile == 1) | (percentile == 0), None).otherwise(df.select(metric).collect()[ci_upp_n][metric])
        
    else:
        point_est = None
        ci_low = None
        ci_upp = None

    if confidence_interval:
        return point_est, ci_low, ci_upp
    else:
        return point_est

def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=[]):
    strg = [str(round(100 * x)) if 100 * x == round(100 * x) else str(100 * x).replace('.', '_') for x in ppt]
    for i in range(len(ppt)):
        if not bycols:
            df = df.withColumn('__', F.lit(1))
            bycols = ['__']
        
        df = percentile_ci(df, metric, ppt[i], confidence_interval)
        
    return df

# Assuming you've converted your pandas DataFrame to a PySpark DataFrame named ed_record_admit_with_ucc_22_spark
ed_record_admit_with_ucc_22_spark = spark.createDataFrame(ed_record_admit_with_ucc_22)

los_regg = calculate_percentile(ed_record_admit_with_ucc_22_spark, 'LOS_HOURS', [0.9], confidence_interval=True)
los_regg.show()



or 

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=[]):
    # Define the window specification
    windowSpec = Window.orderBy(metric)

    # Calculate the percentiles
    for p in ppt:
        # Compute the rank
        rank_col = F.percent_rank().over(windowSpec)
        
        # Calculate the lower and upper indices for the percentile
        pt_low_n = F.floor((df.count() - 1) * p)
        pt_upp_n = F.floor((df.count() - 1) * p) + 1

        # Extract the corresponding values for the lower and upper indices
        pt_low_val = F.expr(f'percentile_approx({metric}, {p})').over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        pt_upp_val = F.expr(f'percentile_approx({metric}, {p})').over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))

        # Compute the point estimate
        point_est = F.when(F.isnull(pt_upp_val), pt_low_val - pt_low_val * (pt_low_n - F.floor(pt_low_n))) \
                        .otherwise(pt_low_val + (pt_upp_val - pt_low_val) * (pt_low_n - F.floor(pt_low_n)))

        # Compute the confidence intervals if requested
        if confidence_interval:
            # Compute the Altman confidence interval
            ci_low_n = F.round(df.count() * p - 1.96 * F.sqrt(df.count() * p * (1 - p))) - 1
            ci_upp_n = F.round(1 + df.count() * p + 1.96 * F.sqrt(df.count() * p * (1 - p))) - 1
            
            # Extract the corresponding values for the confidence intervals
            ci_low_val = F.expr(f'percentile_approx({metric}, {p})').over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
            ci_upp_val = F.expr(f'percentile_approx({metric}, {p})').over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))

            # Compute the confidence intervals
            ci_low = F.when((p == 1) | (p == 0), None).otherwise(ci_low_val)
            ci_upp = F.when((p == 1) | (p == 0), None).otherwise(ci_upp_val)
            
            # Add the columns to the DataFrame
            df = df.withColumn(f'point_estimate_{p}', F.round(point_est * 10000) / 10000)
            df = df.withColumn(f'percentile_{p}_ci_lower', F.when(F.isnull(ci_low), None).otherwise(F.round(ci_low * 10000) / 10000))
            df = df.withColumn(f'percentile_{p}_ci_upper', F.when(F.isnull(ci_upp), None).otherwise(F.round(ci_upp * 10000) / 10000))
        else:
            # Add the column to the DataFrame
            df = df.withColumn(f'point_estimate_{p}', F.round(point_est * 10000) / 10000)

    return df

# Assuming you've converted your pandas DataFrame to a PySpark DataFrame named ed_record_admit_with_ucc_22_spark
ed_record_admit_with_ucc_22_spark = spark.createDataFrame(ed_record_admit_with_ucc_22)

# Call the calculate_percentile function
los_regg = calculate_percentile(ed_record_admit_with_ucc_22_spark, 'LOS_HOURS', [0.9], confidence_interval=True)
los_regg.show()

