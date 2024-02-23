recreate this using pyspark accurately 


def percentile_ci(indata, percentile,confidence_interval=False):
    indata = indata[~indata.isna()].sort_values(ignore_index=True).reset_index(drop=True)
    ct = indata.size
    if ct > 0:
        kf = (ct - 1)*percentile 
        pt_low_n = (np.floor(kf) + 1) - 1
        pt_low_n = np.where(pt_low_n < 0, 0, pt_low_n)
        pt_upp_n = (np.floor(kf) + 2) - 1
        pt_upp_n = np.where(pt_upp_n > ct - 1, ct - 1, pt_upp_n)
        pt_upp_n = np.where(pt_upp_n < 0, 0, pt_upp_n)
        d = kf - np.floor(kf)
        point_est = np.where(pd.isna(indata[pt_upp_n]), indata[pt_low_n] - indata[pt_low_n]*d,\
                           indata[pt_low_n] + d*(indata[pt_upp_n] - indata[pt_low_n]))
        point_est = round(point_est*10000)/10000
    
    # Altman CI
        ci_low_n = round(ct*percentile - 1.96*np.sqrt(ct*percentile*(1 - percentile))) - 1
        ci_low_n = np.where(ci_low_n < 0, 0, ci_low_n)
        ci_upp_n = round(1 + ct*percentile + 1.96*np.sqrt(ct*percentile*(1 - percentile))) - 1
        ci_upp_n = np.where(ci_upp_n > ct - 1, ct - 1, ci_upp_n)
            
        ci_low = np.where(percentile == 1 or percentile == 0, np.NaN, indata[ci_low_n])
        ci_upp = np.where(percentile == 1 or percentile == 0, np.NaN, indata[ci_upp_n])
    else:
        point_est = np.NaN
        ci_low = np.NaN
        ci_upp = np.NaN
        
    if confidence_interval == True:
        return point_est, ci_low, ci_upp 
    else:
        return point_est
        
def calculate_percentile(df, metric, ppt, confidence_interval = False, bycols = ''):
  
    strg = [str(round(100*x)) if 100*x == round(100*x) else str(100*x).replace('.', '_') for x in ppt]
    for i in range(len(ppt)):
        if bycols == '':
            df['__'] = 1
            bycols = '__'
        y = df.groupby(bycols).apply(lambda x: percentile_ci(x[metric], ppt[i], confidence_interval))
        out = pd.DataFrame(y.tolist(), index=y.index).rename(columns = {0:'percentile_' + strg[i], 1:'percentile_' + strg[i] + '_ci_lower', 2 : 'percentile_' + strg[i] + '_ci_upper'}).reset_index()
        if i == 0:
            final = out;
        else:
            final = final.merge(out, on = bycols)
    if bycols == '__':
        final.drop(columns = ['__'], inplace = True)
    return final
