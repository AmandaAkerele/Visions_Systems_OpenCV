import pyspark.pandas as ps

# Read in Spark DataFrame
# spark_df = ps.DataFrame(ed_record_admit_with_ucc_22)

# Define the percentile_ci function using PySpark DataFrame operations
def percentile_ci(indata, percentile, confidence_interval=False):
    indata = indata.dropna().sort_values(ignore_index=True)
    ct = indata.shape[0]
    if ct > 0:
        kf = (ct - 1) * percentile
        pt_low_n = (kf.floor() + 1) - 1
        pt_low_n = pt_low_n.where(pt_low_n >= 0, 0)
        pt_upp_n = (kf.floor() + 2) - 1
        pt_upp_n = pt_upp_n.where(pt_upp_n <= ct - 1, ct - 1)
        pt_upp_n = pt_upp_n.where(pt_upp_n >= 0, 0)
        d = kf - kf.floor()
        point_est = (indata[pt_low_n] - indata[pt_low_n] * d).where(indata[pt_upp_n].isna(), 
                                                                     indata[pt_low_n] + d * (indata[pt_upp_n] - indata[pt_low_n]))
        point_est = (point_est * 10000).round() / 10000

        # Altman CI
        ci_low_n = (ct * percentile - 1.96 * (ct * percentile * (1 - percentile)) ** 0.5) - 1
        ci_low_n = ci_low_n.where(ci_low_n >= 0, 0)
        ci_upp_n = (1 + ct * percentile + 1.96 * (ct * percentile * (1 - percentile)) ** 0.5) - 1
        ci_upp_n = ci_upp_n.where(ci_upp_n <= ct - 1, ct - 1)
        
        ci_low = ci_low_n.where((percentile == 1) | (percentile == 0), indata[ci_low_n])
        ci_upp = ci_upp_n.where((percentile == 1) | (percentile == 0), indata[ci_upp_n])
    else:
        point_est = None
        ci_low = None
        ci_upp = None

    if confidence_interval:
        return point_est, ci_low, ci_upp
    else:
        return point_est

# Define the calculate_percentile function using PySpark DataFrame operations
def calculate_percentile(df, metric, ppt, confidence_interval=False, bycols=''):
    strg = [str(round(100 * x)) if 100 * x == round(100 * x) else str(100 * x).replace('.', '_') for x in ppt]
    for i in range(len(ppt)):
        if bycols == '':
            df['__'] = 1
            bycols = '__'
        y = df.groupby(bycols).apply(lambda x: percentile_ci(x[metric], ppt[i], confidence_interval))
        out = y.tolist()
        index = y.index.tolist()
        out_df = ps.DataFrame(out, index=index).reset_index()
        out_df.columns = [str(col) for col in out_df.columns]
        out_df = out_df.rename(columns={'0': 'percentile_' + strg[i],
                                        '1': 'percentile_' + strg[i] + '_ci_lower',
                                        '2': 'percentile_' + strg[i] + '_ci_upper'})
        if i == 0:
            final = out_df
        else:
            final = final.merge(out_df, on=bycols)

    if bycols == '__':
        final.drop(columns=['__'], inplace=True)

    return final

# Call the calculate_percentile function
los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
los_regg.show()


solve the error below for the code above

---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_1015/1492788117.py in <cell line: 66>()
     64 
     65 # Call the calculate_percentile function
---> 66 los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
     67 los_regg.show()

/tmp/ipykernel_1015/1492788117.py in calculate_percentile(df, metric, ppt, confidence_interval, bycols)
     43     for i in range(len(ppt)):
     44         if bycols == '':
---> 45             df['__'] = 1
     46             bycols = '__'
     47         y = df.groupby(bycols).apply(lambda x: percentile_ci(x[metric], ppt[i], confidence_interval))

TypeError: 'DataFrame' object does not support item assignment
