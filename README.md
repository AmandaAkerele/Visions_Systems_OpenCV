
def percentile_ci_optimized(indata, percentile, confidence_interval=False):
    # Remove NaN values and sort if not already sorted
    clean_data = np.sort(indata[~np.isnan(indata)])

    ct = clean_data.size
    if ct > 0:
        kf = (ct - 1) * percentile
        pt_low_n = max(int(np.floor(kf)), 0)
        pt_upp_n = min(int(np.ceil(kf)), ct - 1)

        d = kf - np.floor(kf)
        point_est = clean_data[pt_low_n] * (1 - d) + clean_data[pt_upp_n] * d
        point_est = round(point_est * 10000) / 10000

        if confidence_interval:
            ci_index = 1.96 * np.sqrt(ct * percentile * (1 - percentile))
            ci_low_n = max(int(np.floor(ct * percentile - ci_index)) - 1, 0)
            ci_upp_n = min(int(np.ceil(ct * percentile + ci_index)) - 1, ct - 1)
            
            ci_low = clean_data[ci_low_n] if percentile not in [0, 1] else np.NaN
            ci_upp = clean_data[ci_upp_n] if percentile not in [0, 1] else np.NaN

            return point_est, ci_low, ci_upp
    else:
        point_est = ci_low = ci_upp = np.NaN

    return point_est if not confidence_interval else (point_est, ci_low, ci_upp)
