# README.md

## Overview
This Python script is designed to execute a data processing pipeline that fetches data from a Redshift database, processes it using Apache Spark, and produces aggregated results along with intermediate datasets. The pipeline focuses on analyzing Emergency Department (ED) visit data across multiple fiscal years to calculate various indicators such as the 90th percentile of wait times and improvement trends.

## Prerequisites
Before running this script, ensure you have the following:
- Python 3.7 or higher
- Apache Spark 3.0 or higher
- AWS Redshift cluster with the necessary data
- Necessary Python packages: `pyspark`, `numpy`, `scipy`, `functools`, `datetime`

## Parameters
The main function `execute_pipeline` accepts the following parameters:
- `environment` (str): The environment to run the pipeline (e.g., 'dev', 'prod').
- `redshift_username` (str): The username for accessing the Redshift cluster.
- `redshift_password` (str): The password for accessing the Redshift cluster.
- `spark` (Optional[SparkSession]): An existing SparkSession instance. If not provided, a new one will be created.
- `years` (Optional[List[int]]): Specific years to process. If not provided, the last five fiscal years will be processed.
- `year_type` (str): The type of year (e.g., fiscal, calendar).
- `reporting_type` (str): The type of reporting (e.g., 'annual', 'quarterly').

## Main Steps
1. **Setup and Configuration**: Initialize necessary configurations and Spark session.
2. **Data Fetching**: Read data from Redshift into Spark DataFrames.
3. **Data Processing**:
   - Filter and group data to calculate numerator and denominator.
   - Calculate specific metrics like UCC percentage and stillbirth flags.
   - Process and aggregate data to calculate various indicators.
4. **Aggregation**: Aggregate data at different levels (corporate, regional, peer group, etc.).
5. **Trend Analysis**: Perform linear regression to identify improvement trends over the years.
6. **Output**: Combine all processed data into a final DataFrame and prepare intermediate datasets for further use.

## Functions
### `execute_pipeline`
Main function to execute the data processing pipeline.

### `read_redshift_into_df`
Helper function to read data from Redshift into a Spark DataFrame.

### `setup_dataframe`
Helper function to create a DataFrame and flag rows based on certain conditions.

### `join_and_filter`
Helper function to join and filter DataFrames based on specific conditions.

### `perform_regression`
UDF to perform linear regression using SciPy.

### `process_trend_data`
Function to process trend data and identify improvement indicators.

### `unify_schema`
Helper function to ensure the schema of all DataFrames is consistent.

## Intermediate Data
The pipeline also generates intermediate datasets stored in a dictionary for further analysis:
- `agg_811`: Aggregated data for indicator 811.
- `tpia_org_combined`: Combined data for corporate level.
- `tpia_reg_combined`: Combined data for regional level.
- `tpia_supp_reg_combined`: Suppressed data for regional level.
- `tpia_supp_corp_combined`: Suppressed data for corporate level.
- `tpia_supp_peer_combined`: Suppressed data for peer group level.

## Running the Pipeline
To run the pipeline, call the `execute_pipeline` function with the necessary parameters. The function will return the final output DataFrame and a dictionary of intermediate dataframes.

```python
output_df, intermediate_data = execute_pipeline(
    environment="prod",
    redshift_username="your_username",
    redshift_password="your_password",
    spark=spark_session,
    years=[2018, 2019, 2020, 2021, 2022],
    year_type="fiscal",
    reporting_type="annual"
)
```

## Conclusion
This script provides a comprehensive data processing pipeline for analyzing Emergency Department visit data. It leverages the power of Apache Spark for large-scale data processing and AWS Redshift for data storage and retrieval. The output and intermediate datasets can be used for further analysis and reporting.
