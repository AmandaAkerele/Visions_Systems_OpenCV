The error you encountered is due to a mismatch between the number of columns you're trying to create in the DataFrame (`stacked_df`) and the actual number of columns in the `stacked_data` you're providing.

Let's break down the issue:

The error says:
```
AssertionError: 5 columns passed, passed data had 4 columns
```

This means that you're trying to create a DataFrame with 5 columns (`['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code']`), but the data you're providing (`stacked_data`) only has 4 columns.

To fix this issue, you should only pass 4 columns when creating the DataFrame `stacked_df`:

Replace this line:

```python
stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code', 'missing_reason_code'])
```

With this one:

```python
stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])
```

Here's the corrected code:

```python
# ...

# Define a function to generate data for a specific year
def generate_data_for_year(year):
    # Create file for shallow slice pilot
    # Indicator: Emergency Department Wait Time for Physician Initial Assessment (90% Spent Less, in Hours)
    EDWT_Indicator_File = EDWT_Indicators[["ORGANIZATION_ID", "improvement_descriptor_code", "compare_descriptor_code", "missing_reason_code"]]
    EDWT_Indicator_File.rename(columns={"ORGANIZATION_ID": "reporting_entity_code"}, inplace=True)

    # Generate random data for metric_result based on the 90th percentile using exponential distribution
    np.random.seed(0)  # Setting seed for reproducibility
    scale_param = 5 # Scale parameter for exponential distribution
    size = len(EDWT_Indicator_File)

    # Generate random data based on the 90th percentile
    random_data = expon.ppf(np.random.rand(size), scale=scale_param)

    # Shift the data to ensure none of the values are zero
    random_data_shifted = random_data + 1

    # Assign the generated data to the dataframe
    EDWT_Indicator_File['metric_result'] = random_data_shifted.round(1)

    # Drop rows with NaN values in the 'metric_result' column
    EDWT_Indicator_File.dropna(subset=['metric_result'], inplace=True)

    # Stack the rows
    stacked_data = []
    for index, row in EDWT_Indicator_File.iterrows():
        stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceTrend', row['improvement_descriptor_code']])
        stacked_data.append([row['reporting_entity_code'], row['metric_result'], 'PerformanceComparison', row['compare_descriptor_code']])

    stacked_df = pd.DataFrame(stacked_data, columns=['reporting_entity_code', 'metric_result', 'metric_descriptor_group_code', 'metric_descriptor_code'])

    # Add remaining columns
    stacked_df['reporting_period_code'] = 'FY20' + str(year)
    stacked_df['reporting_entity_type_code'] = 'ORG'
    stacked_df['indicator_code'] = '811'
    stacked_df['metric_code'] = 'PCTL_90'
    stacked_df['breakdown_type_code_l1'] = 'N/A'
    stacked_df['breakdown_value_code_l1'] = 'N/A'
    stacked_df['breakdown_type_code_l2'] = 'N/A'
    stacked_df['breakdown_value_code_l2'] = 'N/A'
    stacked_df['missing_reason_code'] = ''
    stacked_df['public_metric_result'] = stacked_df['metric_result']

    # Reorder columns
    stacked_df = stacked_df[['reporting_period_code', 'reporting_entity_code', 'reporting_entity_type_code', \
                        'indicator_code', 'metric_code', 'breakdown_type_code_l1', 'breakdown_value_code_l1', 'breakdown_type_code_l2', \
                       'breakdown_value_code_l2', 'metric_result', 'metric_descriptor_group_code', \
                       'metric_descriptor_code', 'missing_reason_code', 'public_metric_result']]

    return stacked_df

# Generate data for each year from FY2018 to FY2022
all_years_data = pd.concat([generate_data_for_year(year) for year in range(18, 23)])

# Write to CSV
# all_years_data.to_csv('DELETE_agg_all_years.csv', index=False)
```

This corrected code should resolve the `AssertionError` you encountered.
