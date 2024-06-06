from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Assuming 'spark' is your SparkSession and 'ed_record_with_ucc_22' is a DataFrame already loaded
# Apply conditions to create the 'tpia_rec' column
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    'tpia_rec',
    F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B') \
    .when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N') \
    .otherwise('Y')
)

# Filter out records where 'tpia_rec' is not 'B'
tpia_reg_cnt = ed_record_with_ucc_22.filter(F.col('tpia_rec') != 'B')

# Group by and aggregate data
tpia_reg_rec = tpia_reg_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'NEW_REGION_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn(
    'tpia_rec_pct',
    F.col('tpia_calc_cnt') / F.col('Total_CASE')
)

# Filter DataFrame based on specific conditions for suppression and reporting
tpia_supp_reg = tpia_reg_rec.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)

tpia_rpt_reg = tpia_reg_rec.filter(
    (F.col('tpia_calc_cnt') >= 50) | ((F.col('tpia_calc_cnt') < 50) & (F.col('tpia_rec_pct') >= 0.75))
)
