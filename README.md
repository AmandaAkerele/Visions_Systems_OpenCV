from pyspark.sql import functions as F

# Update data for specific CORP_ID values
corp_id_mapping = {
    1019: 81170,
    10038: 81124,
    7077: 80960,
    5045: 81131,
    5085: 81180,
    5049: 81263,
    5160: None
}

# Apply the mapping to CORP_ID columns in all DataFrames
dataframes = [los_org_21, los_org_20, los_org_22_a, tpia_org_21, tpia_org_20, tpia_org_22_a]

for i in range(len(dataframes)):
    for col, val in corp_id_mapping.items():
        dataframes[i] = dataframes[i].withColumn('CORP_ID', 
                                                 F.when(F.col('CORP_ID') == col, val).otherwise(F.col('CORP_ID')))
