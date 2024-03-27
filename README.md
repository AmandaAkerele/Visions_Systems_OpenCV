# Display each DataFrame in the los_reg_dfs list
for df in los_reg_dfs:
    df.show()

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Join Example") \
    .getOrCreate()

# Sample DataFrames (replace with your actual DataFrames)
los_org_20 = spark.createDataFrame([(1, 101), (2, 102)], ["CORP_ID", "CORP_PEER"])
los_org_21 = spark.createDataFrame([(1,), (2,)], ["CORP_ID"])
los_org_22_a = spark.createDataFrame([(1,), (2,), (3,)], ["CORP_ID"])
los_org_dfs = [los_org_20, los_org_21, los_org_22_a]

# Function to perform join between two DataFrames
def perform_join(df1, df2, join_columns):
    return df1.join(df2, on=join_columns, how='inner')

# Perform the join for los_org
los_org_3x3 = los_org_dfs[0]
for df in los_org_dfs[1:]:
    common_columns = list(set(los_org_3x3.columns) & set(df.columns))
    los_org_3x3 = perform_join(los_org_3x3, df, common_columns)

# Display the result
los_org_3x3.show()

# Repeat similar process for tpia_org, los_reg, and tpia_reg
# ...

# Stop Spark session
spark.stop()
