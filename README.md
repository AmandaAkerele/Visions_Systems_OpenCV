from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as _sum

# Initialize Spark session
spark = SparkSession.builder.appName("tpia_rec_calculation").getOrCreate()

# Sample DataFrame for demonstration purposes
data = [
    (2023, 'A', 1, 'Y', 'some_value'), 
    (2023, 'B', 2, 'Y', '9999'), 
    (2023, 'C', 3, 'B', None), 
    (2023, 'D', 4, 'Y', ' ')
]
columns = ['SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'AM_CARE_KEY', 'tpia_rec', 'TIME_PHYSICAN_INIT_ASSESSMENT']
tpia_org_cnt = spark.createDataFrame(data, columns)

# Aggregation
tpia_org_rec = tpia_org_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    count('AM_CARE_KEY').alias('Total_CASE'),
    _sum(when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    _sum(when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
)

# Calculate percentage and sort
tpia_org_rec = tpia_org_rec.withColumn('tpia_rec_pct', col('tpia_calc_cnt') / col('Total_CASE'))
tpia_org_rec = tpia_org_rec.orderBy('CORP_ID')

# Conditional DataFrame creation
tpia_supp_org = tpia_org_rec.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
tpia_rpt_org = tpia_org_rec.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))

# Show the results
tpia_supp_org.show()
tpia_rpt_org.show()
