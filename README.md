from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Assuming you already have a SparkSession
spark = SparkSession.builder.appName("example_app").getOrCreate()

# Convert pandas DataFrames to PySpark DataFrames
tpia_org_21 = spark.createDataFrame(tpia_org_21)
tpia_org_20 = spark.createDataFrame(tpia_org_20)
tpia_org_22 = spark.createDataFrame(tpia_org_22)

# Apply transformations
tpia_org_21 = tpia_org_21.withColumn("CORP_ID", 
                                     when(col("CORP_ID") == 1019, 81170)
                                     .when(col("CORP_ID") == 10038, 81124)
                                     .when(col("CORP_ID") == 7077, 80960)
                                     .when(col("CORP_ID") == 5045, 81131)
                                     .when(col("CORP_ID") == 5085, 81180)
                                     .when(col("CORP_ID") == 5049, 81263)
                                     .otherwise(col("CORP_ID")))

tpia_org_20 = tpia_org_20.withColumn("CORP_ID", 
                                     when(col("CORP_ID") == 1019, 81170)
                                     .when(col("CORP_ID") == 10038, 81124)
                                     .when(col("CORP_ID") == 7077, 80960)
                                     .when(col("CORP_ID") == 5045, 81131)
                                     .when(col("CORP_ID") == 5085, 81180)
                                     .when(col("CORP_ID") == 5049, 81263)
                                     .otherwise(col("CORP_ID")))

tpia_org_22 = tpia_org_22.filter(col("CORP_ID") != 5160).withColumnRenamed("percentile_90", "PERCENTILE_90")

# Rename column
tpia_org_21 = tpia_org_21.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")
tpia_org_20 = tpia_org_20.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")
