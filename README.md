from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("ResolveAmbiguity").getOrCreate()

# Sample data for demonstration purposes
data_df_org_dim = [
    (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')
]
columns_df_org_dim = ['org_id', 'corp_id']
df_org_dim = spark.createDataFrame(data_df_org_dim, columns_df_org_dim)

data_ed_nodup_nosb_22 = [
    (1,), (2,), (3,), (4,)
]
columns_ed_nodup_nosb_22 = ['org_id']
ed_nodup_nosb_22 = spark.createDataFrame(data_ed_nodup_nosb_22, columns_ed_nodup_nosb_22)

data_t4 = [
    ('A', 'Other1', 'Type1', 100),
    ('B', 'Other2', 'Type2', 200),
    ('C', 'Other3', 'Type3', 300),
    ('D', 'Other4', 'Type4', 400)
]
columns_t4 = ['corp_id', 'OTHER_COLUMN', 'TYPE', 'FACILITY_AM_CARE_NUM']
t4 = spark.createDataFrame(data_t4, columns_t4)

# Join df_org_dim with ed_nodup_nosb_22 based on org_id
df_fac = df_org_dim.join(ed_nodup_nosb_22, 'org_id')

# Group by org_id and corp_id and count occurrences
tmp_cnt_ed_facility_org = df_fac.groupBy(df_org_dim['org_id'], df_org_dim['corp_id']).agg(count('*').alias('CORP_CNT'))

# Merge t4 with tmp_cnt_ed_facility_org on both org_id and corp_id
ed_facility_org = t4.join(tmp_cnt_ed_facility_org, (t4['corp_id'] == tmp_cnt_ed_facility_org['corp_id']) & (t4['corp_id'] == tmp_cnt_ed_facility_org['corp_id']), 'left')

# Sort the DataFrame by 'TYPE' and 'FACILITY_AM_CARE_NUM'
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Show the result
ed_facility_org.show()
