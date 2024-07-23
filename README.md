pleae accurately solve this error 

---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_6045/4177808757.py in <cell line: 39>()
    331     ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    332         'tpia_rec',
--> 333         when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (col('TIME_PHYSICAN_INIT_ASSESSMENT').rtrim() == ''), 'B')
    334         .when(col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
    335         .otherwise('Y')

TypeError: 'Column' object is not callable
