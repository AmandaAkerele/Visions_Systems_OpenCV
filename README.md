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
        
        point_est = F.when(F.isnull(df.select(metric).collect()[pt_upp_n]), 
                           df.select(metric).collect()[pt_low_n] - df.select(metric).collect()[pt_low_n] * d)\
                      .otherwise(df.select(metric).collect()[pt_low_n] + d * (df.select(metric).collect()[pt_upp_n] - df.select(metric).collect()[pt_low_n]))
        
        point_est = (F.round(point_est * 10000) / 10000).alias(f'point_estimate_{percentile}')
        
        # Altman CI
        ci_low_n = F.round(count * percentile - 1.96 * (count * percentile * (1 - percentile)) ** 0.5) - 1
        ci_low_n = F.when(ci_low_n < 0, 0).otherwise(ci_low_n)
        ci_upp_n = F.round(1 + count * percentile + 1.96 * (count * percentile * (1 - percentile)) ** 0.5) - 1
        ci_upp_n = F.when(ci_upp_n > count - 1, count - 1).otherwise(ci_upp_n)
        
        ci_low = F.when((percentile == 1) | (percentile == 0), None).otherwise(df.select(metric).collect()[ci_low_n])
        ci_upp = F.when((percentile == 1) | (percentile == 0), None).otherwise(df.select(metric).collect()[ci_upp_n])
        
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
