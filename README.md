from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("RenameAndDropDuplicate").getOrCreate()

# Assuming los_org_20, los_org_21, los_org_22_a, los_org_3x3 are already defined DataFrames

# Define a function to rename a duplicated column
def rename_and_drop_duplicate_column(df, column_name, new_name):
    # Check if the column name is duplicated in the DataFrame schema
    if df.columns.count(column_name) > 1:
        # Rename the first occurrence of the column
        columns = [col(c).alias(new_name) if c == column_name else c for c in df.columns]
        df = df.select(*columns)
    # Drop the duplicate column
    df = df.drop(column_name)
    return df

# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.unionByName(los_org_21, allowMissingColumns=True)
    .unionByName(los_org_22_a, allowMissingColumns=True)
    .join(los_org_3x3, on='CORP_ID', how='inner')
    .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Rename one and drop one 'CORP_PEER' column
los_org_all_yr_b = rename_and_drop_duplicate_column(los_org_all_yr_b, "CORP_PEER", "CORP_PEER_NEW")

# Show the result DataFrame structure
los_org_all_yr_b.printSchema()

# Stop the Spark session
spark.stop()
