import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

# Define the percentile_ci function using pandas_udf
@pandas_udf(DoubleType())
def percentile_ci(s, percentile, confidence_interval):
    s = s.dropna().sort_values(ignore_index=True)
    ct = s.shape[0]
    if ct > 0:
        kf = (ct - 1) * percentile
        pt_low_n = (kf.floor() + 1) - 1
        pt_low_n = pt_low_n.where(pt_low_n >= 0, 0)
        pt_upp_n = (kf.floor() + 2) - 1
        pt_upp_n = pt_upp_n.where(pt_upp_n <= ct - 1, ct - 1)
        pt_upp_n = pt_upp_n.where(pt_upp_n >= 0, 0)
        d = kf - kf.floor()
        point_est = (s[pt_low_n] - s[pt_low_n] * d).where(s[pt_upp_n].isna(), 
                                                          s[pt_low_n] + d * (s[pt_upp_n] - s[pt_low_n]))
        point_est = (point_est * 10000).round() / 10000

        # Altman CI
        ci_low_n = (ct * percentile - 1.96 * (ct * percentile * (1 - percentile)) ** 0.5) - 1
        ci_low_n = ci_low_n.where(ci_low_n >= 0, 0)
        ci_upp_n = (1 + ct * percentile + 1.96 * (ct * percentile * (1 - percentile)) ** 0.5) - 1
        ci_upp_n = ci_upp_n.where(ci_upp_n <= ct - 1, ct - 1)
        
        ci_low = ci_low_n.where((percentile == 1) | (percentile == 0), s[ci_low_n])
        ci_upp = ci_upp_n.where((percentile == 1) | (percentile == 0), s[ci_upp_n])
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
        
        y = df.groupby(bycols).apply(lambda x: percentile_ci(x[metric], ppt[i], confidence_interval))
        
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
