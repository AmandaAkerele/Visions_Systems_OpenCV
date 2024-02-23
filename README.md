from pyspark.sql.functions import col, percentile_approx

def calculate_percentile_spark(df, column, percentiles, bycols=None):
    if bycols:
        df = df.groupBy(*bycols).agg(*[percentile_approx(column, percentile).alias(f'percentile_{percentile}') for percentile in percentiles])
    else:
        df = df.agg(*[percentile_approx(column, percentile).alias(f'percentile_{percentile}') for percentile in percentiles])

    return df


from pyspark.sql.functions import upper

# Calculation for los_nt_22
los_nt_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1])
los_nt_22 = los_nt_22.select([upper(col(c)).alias(c) for c in los_nt_22.columns])

# Calculation for los_reg_22
los_reg_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])
los_reg_22 = los_reg_22.select([upper(col(c)).alias(c) for c in los_reg_22.columns])

# Calculation for los_prov_22
los_prov_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'PROVINCE_ID', 'PROVINCE_NAME'])
los_prov_22 = los_prov_22.select([upper(col(c)).alias(c) for c in los_prov_22.columns])

# Calculation for los_peer_22
los_peer_22 = calculate_percentile_spark(ed_record_admit_22_Peer, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'SITE_PEER'])
los_peer_22 = los_peer_22.select([upper(col(c)).alias(c) for c in los_peer_22.columns])

# Calculation for los_org_22
los_org_22 = calculate_percentile_spark(ed_record_admit_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_NAME', 'CORP_PEER'])
los_org_22 = los_org_22.select([upper(col(c)).alias(c) for c in los_org_22.columns])

# Calculation for los_site_22
los_site_22 = calculate_percentile_spark(ed_record_admit_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER'])
los_site_22 = los_site_22.select([upper(col(c)).alias(c) for c in los_site_22.columns])

# Filter and Rename for LOS_site_Huron_Perth
LOS_site_Huron_Perth = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))
LOS_site_Huron_Perth = LOS_site_Huron_Perth.select([upper(col(c)).alias(c) for c in LOS_site_Huron_Perth.columns])

# Additional Filtering and Renaming
filtered_rows = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))
filtered_rows = filtered_rows.select('SUBMISSION_FISCAL_YEAR', col('SITE_ID').alias('CORP_ID'), col('SITE_NAME').alias('CORP_NAME'), col('SITE_PEER').alias('CORP_PEER'), 'PERCENTILE_0.9')

# Concatenate DataFrames
los_org_22 = los_org_22.unionByName(filtered_rows)
