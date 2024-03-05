from pyspark.sql.functions import col

# Alias the DataFrames
tpia_org_22_alias = tpia_org_22.alias("tpia22")
ed_nacrs_flg_1_22_alias = ed_nacrs_flg_1_22.alias("ednacrs22")
tpia_supp_org_alias = tpia_supp_org.alias("tpiasupp")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Join with ed_nacrs_flg_1_22 and filter out matching 'CORP_ID's
tpia_org_filtered_1 = tpia_org_22_alias.join(
    ed_nacrs_flg_1_22_alias, col("tpia22.CORP_ID") == col("ednacrs22.CORP_ID"), "left_anti"
)

# Join with tpia_supp_org and filter out matching 'CORP_ID's
tpia_org_filtered_2 = tpia_org_filtered_1.join(
    tpia_supp_org_alias, col("tpia22.CORP_ID") == col("tpiasupp.CORP_ID"), "left_anti"
)

# Filter ed_facility_org for the specific conditions and select 'CORP_ID'
edfo_filtered = ed_facility_org_alias.filter(
    (col("edfo.SUBMISSION_FISCAL_YEAR") == "2022") &
    ((col("edfo.TYPE") == 'PS') & (col("edfo.CORP_CNT") == 1) |
     (col("edfo.TYPE") == 'DQ') & (col("edfo.CORP_CNT") == 1) & (col("edfo.IND") == 'TPIA'))
).select("edfo.CORP_ID")

# Further filter by joining with the filtered edfo and using left_anti
tpia_org_ta = tpia_org_filtered_2.join(edfo_filtered, col("tpia22.CORP_ID") == col("edfo.CORP_ID"), "left_anti")


or 

from pyspark.sql.functions import col

# Alias the DataFrames for clarity
tpia_org_22_alias = tpia_org_22.alias("tpia22")
ed_nacrs_flg_1_22_alias = ed_nacrs_flg_1_22.alias("ednacrs22")
tpia_supp_org_alias = tpia_supp_org.alias("tpiasupp")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Filter ed_facility_org for the specific conditions
edfo_filtered = ed_facility_org_alias.filter(
    (col("edfo.SUBMISSION_FISCAL_YEAR") == "2022") &
    ((col("edfo.TYPE") == 'PS') & (col("edfo.CORP_CNT") == 1) |
     (col("edfo.TYPE") == 'DQ') & (col("edfo.CORP_CNT") == 1) & (col("edfo.IND") == 'TPIA'))
).select("edfo.CORP_ID")

# Perform a left anti-join to exclude 'CORP_ID's present in ed_nacrs_flg_1_22
tpia_org_excluding_ednacrs = tpia_org_22_alias.join(
    ed_nacrs_flg_1_22_alias, col("tpia22.CORP_ID") == col("ednacrs22.CORP_ID"), "left_anti"
)

# Perform a left anti-join to exclude 'CORP_ID's present in tpia_supp_org
tpia_org_excluding_supp = tpia_org_excluding_ednacrs.join(
    tpia_supp_org_alias, col("tpia22.CORP_ID") == col("tpiasupp.CORP_ID"), "left_anti"
)

# Perform a final left anti-join to exclude 'CORP_ID's based on the edfo_filtered condition
tpia_org_ta = tpia_org_excluding_supp.join(
    edfo_filtered, col("tpia22.CORP_ID") == col("edfo.CORP_ID"), "left_anti"
)



or 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataFrame Alias Resolution") \
    .getOrCreate()

# Assuming tpia_org_22, ed_nacrs_flg_1_22, tpia_supp_org, ed_facility_org are already defined DataFrames

# Alias the DataFrames
tpia_org_22_alias = tpia_org_22.alias("tpia22")
ed_nacrs_flg_1_22_alias = ed_nacrs_flg_1_22.alias("ednacrs22")
tpia_supp_org_alias = tpia_supp_org.alias("tpiasupp")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Prepare the set of conditions for filtering
tpia_corp_conditions = (
    ~col("tpia22.CORP_ID").isin([row.tpia22_CORP_ID for row in ed_nacrs_flg_1_22_alias.select("CORP_ID").collect()]) &
    ~col("tpia22.CORP_ID").isin([row.tpia22_CORP_ID for row in tpia_supp_org_alias.select("CORP_ID").collect()]) &
    ~col("tpia22.CORP_ID").isin(
        [row.edfo_CORP_ID for row in ed_facility_org_alias.filter(
            (col("edfo.SUBMISSION_FISCAL_YEAR") == "2022") &
            ((col("edfo.TYPE") == 'PS') & (col("edfo.CORP_CNT") == 1) |
             (col("edfo.TYPE") == 'DQ') & (col("edfo.CORP_CNT") == 1) & (col("edfo.IND") == 'TPIA'))
        ).select("edfo.CORP_ID").collect()]
    )
)

# Apply the filter conditions
tpia_org_ta = tpia_org_22_alias.filter(tpia_corp_conditions)

# Stop Spark session
spark.stop()




or 
