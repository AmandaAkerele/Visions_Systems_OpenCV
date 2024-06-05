from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("ResolveAmbiguity").getOrCreate()

# Assuming df_org_dim and ed_nodup_nosb_22 are already defined and available
# Sample data for demonstration purposes
data_df_org_dim = [(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')]
columns_df_org_dim = ['org_id', 'corp_id']
df_org_dim = spark.createDataFrame(data_df_org_dim, columns_df_org_dim)

data_ed_nodup_nosb_22 = [(1,), (2,), (3,), (4,)]
columns_ed_nodup_nosb_22 = ['org_id']
ed_nodup_nosb_22 = spark.createDataFrame(data_ed_nodup_nosb_22, columns_ed_nodup_nosb_22)

data_t4 = [(1, 'A', 'Type1', 100), (2, 'B', 'Type2', 200), (3, 'C', 'Type3', 300), (4, 'D', 'Type4', 400)]
columns_t4 = ['corp_id', 'OTHER_COLUMN', 'TYPE', 'FACILITY_AM_CARE_NUM']
t4 = spark.createDataFrame(data_t4, columns_t4)

# Filter df_fac based on org_id present in ed_nodup_nosb_22
df_fac = df_org_dim.join(ed_nodup_nosb_22.select('org_id').distinct(), 'org_id')

# Group by corp_id and count
tmp_cnt_ed_facility_org = df_fac.groupBy('corp_id').agg(count('*').alias('CORP_CNT'))

# Merge t4 with tmp_cnt_ed_facility_org on corp_id
ed_facility_org = t4.join(tmp_cnt_ed_facility_org, on='corp_id', how='inner')

# Sort the DataFrame by 'TYPE' and 'FACILITY_AM_CARE_NUM'
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Show the result
ed_facility_org.show()
