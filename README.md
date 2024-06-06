from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, count

# Assuming 'spark' is your SparkSession and 'ed_record_with_ucc_22' is a DataFrame already loaded

# Creating the 'tpia_rec' column with default value 'B' and then updating based on conditions
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    'tpia_rec',
    when(col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
    .when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNotNull() & (col('TIME_PHYSICAN_INIT_ASSESSMENT') != '9999'), 'Y')
    .otherwise('B')
)

# Filtering out rows with tpia_rec equal to "B"
tpia_org_cnt_ucc_22 = ed_record_with_ucc_22.filter(col('tpia_rec') != 'B')

# Group by and aggregate data
tpia_org_rec_ucc_22 = tpia_org_cnt_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    count('AM_CARE_KEY').alias('Total_CASE'),
    _sum(when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    _sum(when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn(
    'tpia_rec_pct',
    col('tpia_calc_cnt') / col('Total_CASE')
)

# Filter 'tpia_org_rec_ucc' based on conditions
tpia_supp_org_ucc_22 = tpia_org_rec_ucc_22.filter(
    (col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75))
)

tpia_rpt_org_ucc_22 = tpia_org_rec_ucc_22.filter(
    (col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75))
)

# Show results (optional, can be removed or modified per use-case)
tpia_supp_org_ucc_22.show()
tpia_rpt_org_ucc_22.show()
