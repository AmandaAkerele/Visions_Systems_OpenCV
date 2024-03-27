Understood. I'll use the DataFrame names you previously provided and avoid creating new datasets. Here's the code:

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("SuccessiveJoins").getOrCreate()

# Function to perform join between two DataFrames
def perform_join(df1, df2, join_columns):
    return df1.join(df2, on=join_columns, how='inner')

# Define a list of DataFrames
los_org_dfs = [
    los_org_20.select('CORP_ID', 'CORP_PEER'),
    los_org_21.select('CORP_ID'),
    los_org_22_a.select('CORP_ID')
]

# Perform the join for los_org
los_org_3x3 = los_org_dfs[0]
for df in los_org_dfs[1:]:
    common_columns = list(set(los_org_3x3.columns) & set(df.columns))
    los_org_3x3 = perform_join(los_org_3x3, df, common_columns)

# Display the result
los_org_3x3.show()
```

This code will perform the successive inner joins on the provided `los_org` DataFrames and display the resulting DataFrame using Spark.


or 

from functools import reduce
from pyspark.sql import DataFrame

# Function to perform join between two DataFrames
def perform_join(df1, df2, join_columns):
    return df1.join(df2, on=join_columns, how='inner')

# Define a list of DataFrames for los_org
los_org_dfs = [
    los_org_20.select('CORP_ID', 'CORP_PEER'),
    los_org_21.select('CORP_ID'),
    los_org_22_a.select('CORP_ID')
]

# Use reduce to apply inner join function successively
los_org_3x3 = reduce(lambda df1, df2: perform_join(df1, df2, list(set(df1.columns) & set(df2.columns))), los_org_dfs)

# Display the result
los_org_3x3.show()


or 
from pyspark.sql.functions import col

# Define a list of DataFrames for los_org
los_org_dfs = [
    los_org_20.select('CORP_ID', 'CORP_PEER'),
    los_org_21.select('CORP_ID'),
    los_org_22_a.select('CORP_ID')
]

# Start with the first DataFrame in the list
merged_df = los_org_dfs[0]

# Iterate over the remaining DataFrames and perform inner joins
for df in los_org_dfs[1:]:
    # Determine common columns between the current DataFrame and the merged DataFrame
    common_columns = [col_name for col_name in df.columns if col_name in merged_df.columns]
    
    # Perform inner join
    merged_df = merged_df.join(df, on=common_columns, how='inner')

# Display the result
merged_df.show()
