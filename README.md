from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum

# Initialize Spark session
spark = SparkSession.builder.appName("ResolveAmbiguity").getOrCreate()

# Assuming ed_record_admit_noucc is already defined and available
# Sample DataFrame for demonstration purposes
data = [
    (2023, 101, 1, 0.8),
    (2023, 102, None, 0.7),
    (2023, 103, 1, 0.9)
]
columns = ['SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'TIME_PHYSICAN_INIT_ASSESSMENT', 'tpia_rec_pct']
ed_record_admit_noucc = spark.createDataFrame(data, columns)

# Aggregating LOS_org_cnt
tpia_org_cnt2 = ed_record_admit_noucc.groupby('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    sum(when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNotNull(), 1).otherwise(0)).alias('tpia_calc_cnt')
)

# Conditional DataFrame creation
tpia_supp_org2 = tpia_org_cnt2.filter(
    (col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75))
)

tpia_rpt_org2 = tpia_org_cnt2.filter(
    (col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75))
)

# Show the results
tpia_supp_org2.show()
tpia_rpt_org2.show()
