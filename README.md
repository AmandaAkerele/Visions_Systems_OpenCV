from pyspark.sql.functions import col, when, rtrim

# Apply conditions to create the 'tpia_rec' column
ed_record = ed_record.withColumn(
    'tpia_rec',
    when(
        col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (rtrim(col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B'
    ).when(
        col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N'
    ).otherwise('Y')
)
