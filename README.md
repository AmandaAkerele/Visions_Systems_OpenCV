# Assuming 'df' is your large PySpark DataFrame
try:
    row_count = df.count()
    column_count = len(df.columns)
    print("Number of rows:", row_count)
    print("Number of columns:", column_count)
except Exception as e:
    print("Error encountered:", e)



or 

from pyspark.sql import SparkSession

# Assuming you have a SparkSession instance created as 'spark'
# and your DataFrame is named 'df'

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("temp_view")

# Execute SQL query for counting rows
row_count = spark.sql("SELECT COUNT(*) as count FROM temp_view").collect()[0][0]

# Getting column count
column_count = len(df.columns)

# Print results
print("Number of rows:", row_count)
print("Number of columns:", column_count)
