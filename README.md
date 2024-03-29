from pyspark.sql.functions import when, col

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

for df in dataframes:
    for old_id, new_id in corp_id_mapping.items():
        df = df.withColumn("CORP_ID", when(col("CORP_ID") == old_id, new_id).otherwise(col("CORP_ID")))

# Update the original DataFrames with the modified ones
los_org_21, los_org_20, los_org_22_a, tpia_org_21, tpia_org_20, tpia_org_22_a = dataframes

# Rename the 'PEER_GROUP_ID' column to 'CORP_PEER' in los_org_21 and los_org_20 
# & Rename the 'SUBMISSION_FISCAL_YEAR' column to 'FISCAL_YEAR' in los_reg_21 and tpia_reg_21

los_org_21 = los_org_21.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")
los_org_20 = los_org_20.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")
tpia_org_21 = tpia_org_21.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")
tpia_org_20 = tpia_org_20.withColumnRenamed("PEER_GROUP_ID", "CORP_PEER")

# los_reg_21 = los_reg_21.withColumnRenamed("SUBMISSION_FISCAL_YEAR", "FISCAL_YEAR")
# tpia_reg_21 = tpia_reg_21.withColumnRenamed("SUBMISSION_FISCAL_YEAR", "FISCAL_YEAR")
