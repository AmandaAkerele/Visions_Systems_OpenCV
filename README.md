from pyspark.sql.functions import col, percentile_approx

def calculate_percentile_spark(df, column, percentiles, bycols=None):
    if bycols:
        # Calculate percentiles within groups
        df = df.groupBy(*bycols).agg(*[percentile_approx(col(column), percentile).alias(f'percentile_{percentile}') for percentile in percentiles])
    else:
        # Calculate percentiles for the entire DataFrame
        df = df.agg(*[percentile_approx(col(column), percentile).alias(f'percentile_{percentile}') for percentile in percentiles])

    return df


# Calculation for los_reg_22 with grouping
los_reg_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])

# Calculation for los_prov_22 with grouping
los_prov_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'PROVINCE_ID', 'PROVINCE_NAME'])

# Calculation for los_peer_22 with grouping
los_peer_22 = calculate_percentile_spark(ed_record_admit_22_Peer, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'SITE_PEER'])

# Calculation for los_org_22 with grouping
los_org_22 = calculate_percentile_spark(ed_record_admit_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_NAME', 'CORP_PEER'])

# Calculation for los_site_22 with grouping
los_site_22 = calculate_percentile_spark(ed_record_admit_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'SITE_ID', 'SITE_NAME', 'SITE_PEER'])

# Note: For los_nt_22, assuming no grouping is needed
los_nt_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1])

# Addi
