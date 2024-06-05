use inner and left join for the joining of t4 to tmp_cnt_ed_facility_org and ensure the joinig is correct to reflect the corrent count for CORP_CNT

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

df_fac = df_org_dim.join(ed_nodup_nosb_22.select('org_id').distinct(), 'org_id')

# # Group by org_id and count
tmp_cnt_ed_facility_org = df_fac.groupBy('corp_id').agg(count('*').alias('CORP_CNT'))

# Merge result_df with tmp_cnt_ed_facility_org
ed_facility_org = t4.join(tmp_cnt_ed_facility_org, on='corp_id', how='inner')

# Sort the DataFrame by 'TYPE' and 'FACILITY_AM_CARE_NUM'
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Rename columns 'REGION_NAME_x' and 'REGION_NAME_y' to 'REGION_NAME'
# ed_facility_org = ed_facility_org.withColumnRenamed('REGION_NAME_x', 'REGION_NAME').withColumnRenamed('REGION_NAME_y', 'REGION_NAME')

# Remove duplicated columns and reset index
# ed_facility_org = ed_facility_org.dropDuplicates().select([col for col in ed_facility_org.columns if col != 'index'])

# Show the result
ed_facility_org.show()
