# Apply conditions to create the 'tpia_rec' column for ed_record_with_ucc_22
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    'tpia_rec',
    when(
        col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (rtrim(col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B'
    ).when(
        col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N'
    ).otherwise('Y')
)
