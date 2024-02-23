# Assuming you have a list of percentiles to calculate, e.g., [0.9]
percentiles_to_calculate = [0.9]

# A function to apply the calculation to a DataFrame for multiple percentiles
def apply_percentile_calculations(df, column, percentiles):
    results = []
    for percentile in percentiles:
        quantile_value, ci_lower, ci_upper = calculate_percentile_and_ci(df, column, percentile)
        results.append((f'percentile_{percentile}', quantile_value, ci_lower, ci_upper))
    return results

# Applying the calculations

# For los_nt_22
los_nt_22_results = apply_percentile_calculations(los_nt_record_ucc_22, 'LOS_HOURS', percentiles_to_calculate)

# For los_reg_22
los_reg_22_results = apply_percentile_calculations(los_reg_22, 'LOS_HOURS', percentiles_to_calculate)

# For los_prov_22
los_prov_22_results = apply_percentile_calculations(los_prov_22, 'LOS_HOURS', percentiles_to_calculate)

# For los_peer_22
los_peer_22_results = apply_percentile_calculations(los_peer_22, 'LOS_HOURS', percentiles_to_calculate)

# For los_org_22
los_org_22_results = apply_percentile_calculations(los_org_22, 'LOS_HOURS', percentiles_to_calculate)

# For los_site_22
los_site_22_results = apply_percentile_calculations(los_site_22, 'LOS_HOURS', percentiles_to_calculate)

# For LOS_site_Huron_Perth
LOS_site_Huron_Perth_results = apply_percentile_calculations(LOS_site_Huron_Perth, 'LOS_HOURS', percentiles_to_calculate)


or


def calculate_percentile_and_ci(df, column, percentile, confidence_level=1.96):
    quantile_value = df.stat.approxQuantile(column, [percentile], 0.01)[0]
    total_rows = df.count()
    se = math.sqrt(percentile * (1 - percentile) / total_rows)
    ci_lower = quantile_value - confidence_level * se
    ci_upper = quantile_value + confidence_level * se
    return quantile_value, ci_lower, ci_upper

# Applying the function for each DataFrame

# los_nt_22 calculation
los_nt_percentile, los_nt_ci_lower, los_nt_ci_upper = calculate_percentile_and_ci(los_nt_record_ucc_22, 'LOS_HOURS', 0.9)

# los_reg_22 calculation
los_reg_percentile, los_reg_ci_lower, los_reg_ci_upper = calculate_percentile_and_ci(los_reg_22, 'LOS_HOURS', 0.9)

# los_prov_22 calculation
los_prov_percentile, los_prov_ci_lower, los_prov_ci_upper = calculate_percentile_and_ci(los_prov_22, 'LOS_HOURS', 0.9)

# los_peer_22 calculation
los_peer_percentile, los_peer_ci_lower, los_peer_ci_upper = calculate_percentile_and_ci(los_peer_22, 'LOS_HOURS', 0.9)

# los_org_22 calculation
los_org_percentile, los_org_ci_lower, los_org_ci_upper = calculate_percentile_and_ci(los_org_22, 'LOS_HOURS', 0.9)

# los_site_22 calculation
los_site_percentile, los_site_ci_lower, los_site_ci_upper = calculate_percentile_and_ci(los_site_22, 'LOS_HOURS', 0.9)

# LOS_site_Huron_Perth calculation
LOS_site_Huron_Perth_percentile, LOS_site_Huron_Perth_ci_lower, LOS_site_Huron_Perth_ci_upper = calculate_percentile_and_ci(LOS_site_Huron_Perth, 'LOS_HOURS', 0.9)
