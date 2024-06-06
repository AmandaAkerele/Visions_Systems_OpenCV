from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark session initialization (if not already created)
spark = SparkSession.builder.appName("Standalone Data Processing").getMaster("local").getOrCreate()

# Full data as a list of tuples
data = [
    (99012, 29061, 1), (80335, 48006, 2), (80335, 48008, 2), (80337, 48015, 5),
    (80337, 48022, 5), (80337, 48023, 5), (80337, 48024, 5), (80345, 48029, 2),
    (80338, 48032, 3), (80339, 48037, 1), (80340, 48039, 1), (80344, 48044, 1),
    (80341, 48053, 1), (80345, 48063, 2), (80347, 48076, 2), (80348, 48083, 3),
    (80348, 48085, 3), (80348, 48086, 3), (80338, 48116, 3), (80347, 48117, 2),
    (80337, 48120, 5), (80338, 48121, 3), (7043, 71117, 1), (7070, 71163, 1),
    (973, 88050, 1), (5160, 54242, 1), (1006, 88080, 1), (986, 88132, 1),
    (20390, 88142, 1), (99718, 88149, 1), (99724, 88155, 1), (20282, 88349, 1),
    (20400, 88350, 1), (99725, 88391, 1), (99726, 88394, 1), (80226, 88578, 1),
    (80517, 88595, 1), (99768, 88922, 1)
]

# Filtering data before creating DataFrame to exclude '54242'
filtered_data = [row for row in data if row[1] != 54242]

# Creating DataFrame from the filtered data
df = spark.createDataFrame(filtered_data, ['CORP_ID', 'FACILITY_AM_CARE_NUM', 'CNT_SITE'])

# Cache the DataFrame to optimize multiple actions
df.cache()

# Show the DataFrame structure and preview some data
df.printSchema()
df.show(truncate=False)

# Count the number of entries in the DataFrame
count_entries = df.count()
print("Number of entries after filtering: ", count_entries)

# If the cached DataFrame is no longer needed, it's a good practice to unpersist it
df.unpersist()
