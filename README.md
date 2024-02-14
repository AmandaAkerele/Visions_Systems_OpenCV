# After your existing code

# Cache the DataFrame if it's going to be reused
nacrs_yr_df.cache()

# Get the row count
row_count = nacrs_yr_df.count()

# Print the row count
print("Row count:", row_count)

# Clear the cache if the DataFrame is no longer needed
nacrs_yr_df.unpersist()
