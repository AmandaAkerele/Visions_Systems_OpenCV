from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as sum_col, expr, trim

# Initialize Spark session
spark = SparkSession.builder.appName("tpia_calculation").getOrCreate()

# Assuming ed_record and ed_record_with_ucc_22 are already loaded as Spark DataFrames

# For corp level without UCC
ed_record = ed_record.withColumn(
    'tpia_rec',
    when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (trim(col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B')
    .when(col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
    .otherwise('Y')
)

# Filter records not equal to 'B'
tpia_org_cnt = ed_record.filter(col('tpia_rec') != 'B')

# Aggregation
tpia_org_rec = tpia_org_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    count('AM_CARE_KEY').alias('Total_CASE'),
    sum_col(when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    sum_col(when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
)

# Calculate percentage and sort
tpia_org_rec = tpia_org_rec.withColumn('tpia_rec_pct', col('tpia_calc_cnt') / col('Total_CASE'))
tpia_org_rec = tpia_org_rec.orderBy('CORP_ID')

# Conditional DataFrame creation
tpia_supp_org = tpia_org_rec.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
tpia_rpt_org = tpia_org_rec.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))

# For corp level with UCC
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    'tpia_rec',
    when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (trim(col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B')
    .when(col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
    .otherwise('Y')
)

# Filter out rows with tpia_rec_22 equal to "B"
tpia_org_cnt_ucc_22 = ed_record_with_ucc_22.filter(col('tpia_rec') != 'B')

# Aggregation
tpia_org_rec_ucc_22 = tpia_org_cnt_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    count('AM_CARE_KEY').alias('Total_CASE'),
    sum_col(when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    sum_col(when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn('tpia_rec_pct', col('tpia_calc_cnt') / col('Total_CASE'))

# Filter 'tpia_org_rec_ucc' based on conditions
tpia_supp_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
tpia_rpt_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))

# Display the results
tpia_supp_org.show()
tpia_rpt_org.show()
tpia_supp_org_ucc_22.show()
tpia_rpt_org_ucc_22.show()
