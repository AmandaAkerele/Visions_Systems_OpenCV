from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("HandleAmbiguity").getOrCreate()

# Assuming DataFrame initialization and imports are done

def handle_ambiguous_columns(df, ambiguous_col_name, new_name):
    # Temporarily rename columns to avoid ambiguity
    temp_col_names = {col_name: f"{col_name}_{idx}" for idx, col_name in enumerate(df.columns, 1) if col_name == ambiguous_col_name}
    
    # Rename to temporary unique names
    for original, temp in temp_col_names.items():
        df = df.withColumnRenamed(original, temp)

    # Now safely rename the first occurrence (which we assume is the one to keep)
    first_temp_col = next(iter(temp_col_names.values()))  # Get first item in dictionary values
    df = df.withColumnRenamed(first_temp_col, new_name)

    # Drop other temporary columns if any
    for temp in temp_col_names.values():
        if temp != first_temp_col:  # Avoid dropping the renamed valid column
            df = df.drop(temp)

    return df

# Concatenate, merge, and prepare for renaming and dropping duplicates
los_org_all_yr_b = (
    los_org_20.unionByName(los_org_21, allowMissingColumns=True)
    .unionByName(los_org_22_a, allowMissingColumns=True)
    .join(los_org_3x3, on='CORP_ID', how='inner')
    .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Handle ambiguous columns by renaming one and dropping the others
los_org_all_yr_b = handle_ambiguous_columns(los_org_all_yr_b, "CORP_PEER", "CORP_PEER_NEW")

# Show the result DataFrame structure
los_org_all_yr_b.printSchema()

# Stop the Spark session
spark.stop()
