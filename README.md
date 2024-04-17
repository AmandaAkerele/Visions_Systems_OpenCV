from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
                    .appName("Handle Duplicate Columns") \
                    .getOrCreate()

# Example DataFrame loading and renaming duplicate columns
los_org_all_yr_b = spark.read.csv("path_to_your_data.csv", header=True, inferSchema=True)
los_org_all_yr_b = los_org_all_yr_b.toDF(*[col if idx != 1 else "CORP_PEER_RENAMED" for idx, col in enumerate(los_org_all_yr_b.columns)])

# Drop the duplicated column after renaming
los_org_all_yr_b = los_org_all_yr_b.drop("CORP_PEER_RENAMED")

# Dropping duplicates
los_org_all_yr_b = los_org_all_yr_b.dropDuplicates(["CORP_PEER"])

# Show the result
los_org_all_yr_b.show()
