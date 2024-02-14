from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("NACRS Data Processing").getOrCreate()

# ... your existing code for loading and processing the data ...

# Use countApprox for getting approximate row count
# For example, using a timeout of 1000ms (1 second) and a confidence of 0.95
approx_row_count = nacrs_yr_df.countApprox(1000, 0.95)
print("Approximate Row Count:", approx_row_count)
