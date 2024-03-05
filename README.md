from pyspark.sql.functions import col

# Sort the DataFrame by CORP_ID
ed_nacrs_flg_1_22 = ed_nacrs_flg_1_22.orderBy('CORP_ID')

# Create DataFrame of excluded CORP_IDs
excluded_corp_ids = ed_nacrs_flg_1_22.select('CORP_ID').union(
    tpia_supp_org.select('CORP_ID')
).union(
    ed_facility_org.filter(
        (col('SUBMISSION_FISCAL_YEAR') == "2022") &
        (((col('TYPE') == 'PS') & (col('CORP_CNT') == 1)) |
         ((col('TYPE') == 'DQ') & (col('CORP_CNT') == 1) & (col('IND') == 'TPIA')))
    ).select('CORP_ID')
).distinct()

# Remove suppressed corp and non-reported corps for TPIA
tpia_org_ta = tpia_org_22.join(excluded_corp_ids, 'CORP_ID', 'left_anti')

# Remove suppressed corp and non-reported corps for ELOS
los_org_ta = los_org_22.join(excluded_corp_ids, 'CORP_ID', 'left_anti')

# Remove specific CORP_ID and check for not null in CORP_PEER
tpia_org_ta = tpia_org_ta.filter((col('CORP_ID') != 80228) & col('CORP_PEER').isNotNull())
los_org_ta = los_org_ta.filter((col('CORP_ID') != 80228) & col('CORP_PEER').isNotNull())

# Sort DataFrames
tpia_org_ta = tpia_org_ta.orderBy('CORP_ID')
los_org_ta = los_org_ta.orderBy('CORP_ID')

# For the percentile calculations, assuming the percentile function is available (e.g., using a UDF or built-in function)
from pyspark.sql.functions import expr

# Calculate percentile for TPIA
prct_20_80_tpia_org_22_ta = tpia_org_ta.groupBy('CORP_PEER') \
    .agg(expr('percentile_approx(PERCENTILE_90, array(0.2, 0.8))').alias('percentiles')) \
    .selectExpr('CORP_PEER', 'percentiles[0] as 20th_Percentile', 'percentiles[1] as 80th_Percentile')

# Calculate percentile for ELOS
prct_20_80_los_org_22_ta = los_org_ta.groupBy('CORP_PEER') \
    .agg(expr('percentile_approx(PERCENTILE_90, array(0.2, 0.8))').alias('percentiles')) \
    .selectExpr('CORP_PEER', 'percentiles[0] as 20th_Percentile', 'percentiles[1] as 80th_Percentile')

# Similar approach for regional values



orr

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataFrame Transformation") \
    .getOrCreate()

# Assuming tpia_org_22, ed_nacrs_flg_1_22, tpia_supp_org, ed_facility_org, los_org_22, los_supp_org_22, tpia_reg_22_ta, and los_reg_22_ta are already defined DataFrames

# Sort the resulting DataFrame by CORP_ID
ed_nacrs_flg_1_22 = ed_nacrs_flg_1_22.orderBy('CORP_ID')

# Remove suppressed corp and non-reported corps for TPIA
tpia_corp_conditions = ~tpia_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22.select('CORP_ID')) & \
                       ~tpia_org_22['CORP_ID'].isin(tpia_supp_org.select('CORP_ID')) & \
                       ~tpia_org_22['CORP_ID'].isin(
                           ed_facility_org.filter(
                               (col("SUBMISSION_FISCAL_YEAR") == "2022") &
                               ((col("TYPE") == 'PS') & (col("CORP_CNT") == 1) |
                                (col("TYPE") == 'DQ') & (col("CORP_CNT") == 1) & (col("IND") == 'TPIA'))
                           ).select("CORP_ID")
                       )
tpia_org_ta = tpia_org_22.filter(tpia_corp_conditions)

# Remove suppressed corp and non-reported corps for ELOS
los_corp_conditions = ~los_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22.select('CORP_ID')) & \
                      ~los_org_22['CORP_ID'].isin(los_supp_org_22.select('CORP_ID')) & \
                      ~los_org_22['CORP_ID'].isin(
                          ed_facility_org.filter(
                              (col("SUBMISSION_FISCAL_YEAR") == "2022") &
                              ((col("TYPE") == 'PS') & (col("CORP_CNT") == 1) |
                               (col("TYPE") == 'DQ') & (col("CORP_CNT") == 1) & (col("IND") == 'ELOS'))
                          ).select("CORP_ID")
                      )
los_org_ta = los_org_22.filter(los_corp_conditions)

# Remove Huron Perth Healthcare Alliance, corp_id = 80228 from TPIA and ELOS
tpia_org_ta = tpia_org_ta.filter((col('CORP_ID') != 80228) & tpia_org_ta['CORP_PEER'].isNotNull())
los_org_ta = los_org_ta.filter((col('CORP_ID') != 80228) & los_org_ta['CORP_PEER'].isNotNull())

# Sort DataFrames
tpia_org_ta = tpia_org_ta.orderBy('CORP_ID')
los_org_ta = los_org_ta.orderBy('CORP_ID')

# Calculate percentile for TPIA
prct_20_80_tpia_org_22_ta = tpia_org_ta.groupBy('CORP_PEER').agg({'PERCENTILE_90': 'quantile'}).alias('prct_20_80_tpia_org_22_ta')

# Calculate percentile for ELOS
prct_20_80_los_org_22_ta = los_org_ta.groupBy('CORP_PEER').agg({'PERCENTILE_90': 'quantile'}).alias('prct_20_80_los_org_22_ta')

# Stop Spark session
spark.stop()
