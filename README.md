from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Assuming 'spark' is your SparkSession
spark = SparkSession.builder.appName("Data Filtering").getOrCreate()

# Full data as a list of tuples to create DataFrame SL
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

# Creating initial DataFrame from the data
SL = spark.createDataFrame(data, ['CORP_ID', 'FACILITY_AM_CARE_NUM', 'CNT_SITE'])

# Filtering data to exclude '54242' and creating SL_filtered DataFrame
SL_filtered = SL.filter(col('FACILITY_AM_CARE_NUM') != 54242)

# Show the DataFrame structure and preview some data for both DataFrames
SL.printSchema()
SL.show(truncate=False)
SL_filtered.show(truncate=False)

# Count the number of entries in the filtered DataFrame
count_entries = SL_filtered.count()
print("Number of entries after filtering: ", count_entries)
