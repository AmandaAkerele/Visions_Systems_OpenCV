from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Ensure SparkSession is initialized
spark = SparkSession.builder.appName("Provincial Percentile Calculation").getOrCreate()

# Perform aggregation to calculate the 90th percentile
tpia_prov_22 = TPIA_nt_record_ucc_df.groupBy(
    'SUBMISSION_FISCAL_YEAR',
    'adm_prov_id', 
    'adm_prov_plldj_name_e_desc'
).agg(
    F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90')
)

# Filter out rows where 'adm_prov_id' is null after aggregation
tpia_prov_22 = tpia_prov_22.filter(tpia_prov_22['adm_prov_id'].isNotNull())

# Show the result
tpia_prov_22.show()
