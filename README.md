from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Assuming the Spark session is already started
spark = SparkSession.builder.appName("Data Merging and Transformation").getOrCreate()

# Assume los_org_20, los_org_21, los_org_22, los_reg_20, los_reg_21, los_reg_22 are already loaded as DataFrames

# Merge operations similar to pd.merge() on 'CORP_ID' and 'REGION_ID'
los_org_3x3_a = los_org_20.select("CORP_ID", "CORP_PEER").join(los_org_21.select("CORP_ID"), on="CORP_ID", how="inner")
los_org_3x3 = los_org_3x3_a.join(los_org_22.select("CORP_ID"), on="CORP_ID", how="inner")

los_reg_3x3_a = los_reg_20.select("REGION_ID").join(los_reg_21.select("REGION_ID"), on="REGION_ID", how="inner")
los_reg_3x3 = los_reg_3x3_a.join(los_reg_22.select("REGION_ID"), on="REGION_ID", how="inner")

# Union operation similar to pd.concat()
los_org_all_yr = los_org_20.unionByName(los_org_21).unionByName(los_org_22)
los_reg_all_yr = los_reg_20.unionByName(los_reg_21).unionByName(los_reg_22)

# Merging all year data with 3x3 data
los_org_all_yr_a = los_org_all_yr.join(los_org_3x3.select("CORP_ID"), on="CORP_ID", how="inner")
los_reg_all_yr_a = los_reg_all_yr.join(los_reg_3x3.select("REGION_ID"), on="REGION_ID", how="inner")

# Rename column 'FISCAL_YEAR' to 'TIME'
los_org_all_yr_b = los_org_all_yr_a.withColumnRenamed("FISCAL_YEAR", "TIME")
los_reg_all_yr_b = los_reg_all_yr_a.withColumnRenamed("FISCAL_YEAR", "TIME")

# Replicate the operations for the tpia datasets
tpia_org_3x3_a = tpia_org_20.select("CORP_ID", "CORP_PEER").join(tpia_org_21.select("CORP_ID"), on="CORP_ID", how="inner")
tpia_org_3x3 = tpia_org_3x3_a.join(tpia_org_22.select("CORP_ID"), on="CORP_ID", how="inner")

tpia_reg_3x3_a = tpia_reg_20.select("REGION_ID").join(tpia_reg_21.select("REGION_ID"), on="REGION_ID", how="inner")
tpia_reg_3x3 = tpia_reg_3x3_a.join(tpia_reg_22.select("REGION_ID"), on="REGION_ID", how="inner")

tpia_org_all_yr = tpia_org_20.unionByName(tpia_org_21).unionByName(tpia_org_22)
tpia_reg_all_yr = tpia_reg_20.unionByName(tpia_reg_21).unionByName(tpia_reg_22)

tpia_org_all_yr_a = tpia_org_all_yr.join(tpia_org_3x3.select("CORP_ID"), on="CORP_ID", how="inner")
tpia_reg_all_yr_a = tpia_reg_all_yr.join(tpia_reg_3x3.select("REGION_ID"), on="REGION_ID", how="inner")

tpia_org_all_yr_b = tpia_org_all_yr_a.withColumnRenamed("FISCAL_YEAR", "TIME")
tpia_reg_all_yr_b = tpia_reg_all_yr_a.withColumnRenamed("FISCAL_YEAR", "TIME")
