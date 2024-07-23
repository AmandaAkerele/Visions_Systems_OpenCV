from pyspark.sql.functions import col, when, trim

ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    'tpia_rec',
    when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (trim(col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B')
    .when(col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
    .otherwise('Y')
)
