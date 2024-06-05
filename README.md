from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim

# Initialize Spark session
spark = SparkSession.builder.appName("tpia_rec_calculation").getOrCreate()

# Sample DataFrame for demonstration purposes
data = [
    (1, 'Y', 'some_value'), 
    (2, 'Y', '9999'), 
    (3, 'Y', None), 
    (4, 'Y', ' ')
]
columns = ['ID', 'OTHER_COLUMN', 'TIME_PHYSICAN_INIT_ASSESSMENT']
ed_record = spark.createDataFrame(data, columns)

# For corp level without UCC
# Vectorized approach for 'tpia_rec' calculation
ed_record = ed_record.withColumn(
    'tpia_rec',
    when(
        col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (trim(col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''),
        'B'
    ).when(
        col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999',
        'N'
    ).otherwise('Y')
)

# Filter records not equal to 'B'
tpia_org_cnt = ed_record.filter(col('tpia_rec') != 'B')

# Show the result
tpia_org_cnt.show()
