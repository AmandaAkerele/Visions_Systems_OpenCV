using this code, group by org_id and corp_id and then finally join t4.join(tmp_cnt_ed_facility_org) to both org_id and corp_id
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Filter df_fac based on org_id present in ed_nodup_nosb_22
df_fac = df_org_dim.join(ed_nodup_nosb_22.select('org_id').distinct(), 'org_id')

# Group by corp_id and count
tmp_cnt_ed_facility_org = df_fac.groupBy('org_id').agg(count('*').alias('CORP_CNT'))

# Merge t4 with tmp_cnt_ed_facility_org on corp_id
ed_facility_org = t4.join(tmp_cnt_ed_facility_org, on='org_id', how='left')

# Sort the DataFrame by 'TYPE' and 'FACILITY_AM_CARE_NUM'
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Show the result
ed_facility_org.show()
