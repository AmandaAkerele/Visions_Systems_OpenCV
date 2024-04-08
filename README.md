import pyspark.sql.functions as F
from pyspark.sql import Window

# Convert pandas DataFrame to PySpark DataFrame
spark_df = df.pandas_api()

def percentile_ci(indata, percentile, confidence_interval=False):
    indata = indata.na.drop().orderBy(indata).withColumn("index", F.monotonically_increasing_id())
    ct = indata.count()
    
    if ct > 0:
        kf = (ct - 1) * percentile
        pt_low_n = F.floor(kf) + 1 - 1
        pt_low_n = F.when(pt_low_n < 0, 0).otherwise(pt_low_n)
        
        pt_upp_n = F.floor(kf) + 2 - 1
        pt_upp_n = F.when(pt_upp_n > ct - 1, ct - 1).otherwise(pt_upp_n)
        d = kf - F.floor(kf)
        
        point_est = F.when(F.isnull(indata.collect()[pt_upp_n]["value"]), 
                           indata.collect()[pt_low_n]["value"] - indata.collect()[pt_low_n]["value"] * d) \
                      .otherwise(indata.collect()[pt_low_n]["value"] + d * (indata.collect()[pt_upp_n]["value"] - indata.collect()[pt_low_n]["value"]))
        
        point_est = F.round(point_est * 10000) / 10000

        ci_low_n = F.round(ct * percentile - 1.96 * F.sqrt(ct * percentile * (1 - percentile))) - 1
        ci_low_n = F.when(ci_low_n < 0, 0).otherwise(ci_low_n)
        
        ci_upp_n = F.round(1 + ct * percentile + 1.96 * F.sqrt(ct * percentile * (1 - percentile))) - 1
        ci_upp_n = F.when(ci_upp_n > ct - 1, ct - 1).otherwise(ci_upp_n)
        
        ci_low = F.when((percentile == 1) | (percentile == 0), F.nan()).otherwise(indata.collect()[ci_low_n]["value"])
        ci_upp = F.when((percentile == 1) | (percentile == 0), F.nan()).otherwise(indata.collect()[ci_upp_n]["value"])
    else:
        point_est = F.nan()
        ci_low = F.nan()
        ci_upp = F.nan()
    
    if confidence_interval:
        return point_est, ci_low, ci_upp 
    else:
        return point_est

def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=None):
    strg = [str(round(100*x)) if 100*x == round(100*x) else str(100*x).replace('.', '_') for x in ppt]
    
    if bycols is None:
        bycols = ['__']
        df = df.withColumn("__", F.lit(1))
    
    final = None
    for i in range(len(ppt)):
        y = df.groupBy(*bycols).agg(F.expr(f"percentile_ci({metric}, {ppt[i]}, {confidence_interval})").alias("result"))
        y = y.select(*bycols, "result.*")
        y = y.select(*y.columns, F.col("result.*"))
        
        out = y.select(*bycols, 
                       F.col(f"point_est").alias(f"percentile_{strg[i]}"), 
                       F.col(f"ci_low").alias(f"percentile_{strg[i]}_ci_lower"), 
                       F.col(f"ci_upp").alias(f"percentile_{strg[i]}_ci_upper"))
        
        if final is None:
            final = out
        else:
            final = final.join(out, on=bycols, how='inner')
    
    if '__' in bycols:
        final = final.drop("__")
    
    return final

# Below shows some examples of how the function may be called
# calculate_percentile(spark_df, 'los_hours', [0, 0.5, 0.9, 0.999, 1], confidence_interval=True)
# los_reg = calculate_percentile(spark_df, 'los_hours', ppt=[0.9], bycols=['fiscal_year', 'facility_province', 'region_id', 'region_name'])
# calculate_percentile(los_reg, 'percentile_90', ppt=[0.2, 0.8], bycols=['fiscal_year'])
# calculate_percentile(spark_df, 'wait_time_to_pia_hours', ppt=[0.9], bycols=['fiscal_year', 'facility_province', 'region_id', 'region_name'])
# los_reg_20_80 = calculate_percentile(spark_df, 'percentile_90', [0.2, 0.8])
# tpia_org_peer_20_80 = calculate_percentile(spark_df, 'percentile_90', ppt=[0.2, 0.8], bycols=['fiscal_year', 'peer_group_id'])
