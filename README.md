    'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', 'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

    return stacked_df

# Generate data for each year from FY2018 to FY2022
all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

# Write to CSV
# all_years_data.to_csv('DELETE_agg_all_years.csv', index=False)
