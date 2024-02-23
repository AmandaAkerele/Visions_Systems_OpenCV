from pyspark.sql.functions import col, percentile_approx

def calculate_percentile_spark(df, column, percentiles, bycols=None):
    if bycols:
        # Calculate percentiles within groups
        df = df.groupBy(*bycols).agg(*[percentile_approx(col(column), percentile).alias(f'percentile_{percentile}') for percentile in percentiles])
    else:
        # Calculate percentiles for the entire DataFrame
        df = df.agg(*[percentile_approx(col(column), percentile).alias(f'percentile_{percentile}') for percentile in percentiles])

    return df




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
    '''
    Author: Mingyang Li
    Date:  August 18, 2023
    Version: v1    
    Input:
          df: Python dataframe
          metric: A numerical column to based percentile number upon, only one column 
              is allowed, eg. 'los_hours', 'percentile_90', 'wait_time_to_pia_hours'
          ppt: a list of float numbers between 0 and 1, 
              eg. [0.9], [0.2, 0.8], [1], [0, 0.1, 0.9975]
              there is no limit on the number of percentiles to be calculated, 
              also no limit on the number of decimals to be included.
          confidence_interval: True or False to indicate whether confidence interval 
              will be returned, the default value is False.    
          bycols: a list of columns that define the groups on which the percentile will
              be calculated. eg. ['fiscal_year','peer_group_id'],['fiscal_year']
              There is no limit on how many columns to be included.
              If the percenties are not calcualted by groups, then please don't specify 
              anything. No groupings will be applied by default. 
    output: 
          a Python dataframe with percentile estiamte and its 95% two-sided confidence 
          limits (if confidence_interval = True) by groups (if applicable)
          if multiple percentiles to be calculated, all percentiles estimates will be 
          on the same row.
    '''  
  
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











    los_nt_22=calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS', [0,0.5,0.9,0.999,1],confidence_interval=True)
los_nt_22=los_nt_22.rename(columns=lambda x: x.upper())

los_reg_22=calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','FACILITY_PROVINCE','NEW_REGION_ID','REGION_E_DESC'])
los_reg_22=los_reg_22.rename(columns=lambda x: x.upper())

los_prov_22=calculate_percentile(los_nt_record_ucc_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','FACILITY_PROVINCE','PROVINCE_ID','PROVINCE_NAME'])
los_prov_22=los_prov_22.rename(columns=lambda x: x.upper())

los_peer_22=calculate_percentile(ed_record_admit_22_Peer, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','SITE_PEER'])
los_peer_22=los_peer_22.rename(columns=lambda x: x.upper())

los_org_22=calculate_percentile(ed_record_admit_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','CORP_ID','CORP_NAME','CORP_PEER'])
los_org_22=los_org_22.rename(columns=lambda x: x.upper())

los_site_22=calculate_percentile(ed_record_admit_22, 'LOS_HOURS',ppt= [0.9],bycols=['SUBMISSION_FISCAL_YEAR','SITE_ID','SITE_NAME','SITE_PEER'])
los_site_22=los_site_22.rename(columns=lambda x: x.upper())

LOS_site_Huron_Perth = los_site_22[los_site_22['SITE_ID'].isin([5096,5099,5103,5209])]
LOS_site_Huron_Perth=LOS_site_Huron_Perth.rename(columns=lambda x: x.upper())

# Filter tpia_site DataFrame based on the condition
filtered_rows = los_site_22[los_site_22['SITE_ID'].isin([5096, 5099, 5103, 5209])]

# Select only the required columns
filtered_rows = filtered_rows[['SUBMISSION_FISCAL_YEAR', 'SITE_ID','SITE_NAME', 'SITE_PEER', 'PERCENTILE_90']]

# Rename columns to match the tpia_org DataFrame structure
filtered_rows = filtered_rows.rename(columns={'SITE_ID': 'CORP_ID', 'SITE_NAME':'CORP_NAME', 'SITE_PEER':'CORP_PEER'})

los_org_22=pd.concat([los_org_22, filtered_rows], ignore_index=True)
