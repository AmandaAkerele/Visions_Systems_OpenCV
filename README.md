from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("RemoveDuplicateColumns").getOrCreate()

# Assuming 'los_org_all_yr_b' is your original DataFrame
# Let's print the current columns to see the duplicates
print("Original Columns:", los_org_all_yr_b.columns)

# Finding duplicate column names
from collections import Counter
col_counts = Counter(los_org_all_yr_b.columns)
duplicate_columns = [col for col, count in col_counts.items() if count > 1]
print("Duplicate Columns Found:", duplicate_columns)

# If you prefer to drop duplicates (assuming the second occurrence is the one to drop)
if duplicate_columns:
    # Selecting columns while dropping duplicates (change logic here as needed)
    columns_to_select = [col if col_counts[col] == 1 else col + "_renamed" for col in los_org_all_yr_b.columns]
    los_org_all_yr_b = los_org_all_yr_b.toDF(*columns_to_select)

    # Check if renaming has resolved the issue
    col_counts = Counter(los_org_all_yr_b.columns)
    still_duplicates = [col for col, count in col_counts.items() if count > 1]
    if still_duplicates:
        print("Still have duplicates, needs manual inspection:", still_duplicates)
    else:
        print("Duplicates resolved, new columns:", los_org_all_yr_b.columns)
else:
    print("No duplicate columns to handle.")

# Now, you can safely perform operations like groupBy and aggregation
# Example: Applying a function or simply displaying the DataFrame
los_org_all_yr_b.show()
