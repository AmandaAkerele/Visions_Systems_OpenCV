---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_1015/1753866750.py in <cell line: 59>()
     57 
     58 
---> 59 los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
     60 los_regg.show()

/tmp/ipykernel_1015/1753866750.py in calculate_percentile(df, metric, ppt, confidence_interval, bycols)
     51             bycols = ['__']
     52 
---> 53         df = percentile_ci(df, metric, ppt[i], confidence_interval)
     54 
     55     return df

/tmp/ipykernel_1015/1753866750.py in percentile_ci(df, metric, percentile, confidence_interval)
     19         d = kf - F.floor(F.lit(kf))
     20 
---> 21         point_est = F.when(F.isnull(df.select(metric).collect()[pt_upp_n]), 
     22                            df.select(metric).collect()[pt_low_n] - df.select(metric).collect()[pt_low_n] * d)\
     23                       .otherwise(df.select(metric).collect()[pt_low_n] + d * (df.select(metric).collect()[pt_upp_n] - df.select(metric).collect()[pt_low_n]))
