# Find common columns
common_columns = set(tmp_ed_facility_org_a.columns) & set(t3.columns) & set(t4.columns)

tmp_ed_facility_org_a_common = tmp_ed_facility_org_a.select(*common_columns)
t3_common = t3.select(*common_columns)
t4_common = t4.select(*common_columns)



tmp_ed_facility_org = tmp_ed_facility_org_a_common.union(t3_common).union(t4_common).distinct()



or 

from pyspark.sql.functions import lit

# List of all column names in tmp_ed_facility_org_a
all_columns = tmp_ed_facility_org_a.columns

# Add missing columns to t3 and t4
for col in all_columns:
    if col not in t3.columns:
        t3 = t3.withColumn(col, lit(None))
    if col not in t4.columns:
        t4 = t4.withColumn(col, lit(None))

tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3).unionByName(t4).distinct()












ed_facility_org = tmp_ed_facility_org.join(tmp_cnt_ed_facility_org, 'CORP_ID')
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])





