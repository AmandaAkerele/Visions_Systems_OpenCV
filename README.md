from pyspark.sql import SparkSession

# Create a SparkSession with increased executor memory and potentially other settings
spark = SparkSession.builder \
    .appName("Large DataFrame Application") \
    .config("spark.executor.memory", "4g")  # Increase as needed
    .config("spark.executor.cores", "4")  # Adjust based on your cluster setup
    .config("spark.driver.memory", "4g")  # Increase as needed
    .config("spark.sql.shuffle.partitions", "200")  # Adjust based on your data size
    .getOrCreate()

# Assuming df is your DataFrame
try:
    # Row count
    row_count = df.count()
    print("Number of rows:", row_count)

    # Column count
    column_count = len(df.columns)
    print("Number of columns:", column_count)

except Exception as e:
    print("Error encountered:", e)
    spark.stop()

