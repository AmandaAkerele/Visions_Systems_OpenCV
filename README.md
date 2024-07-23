solve error accurately

                                                                              
---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_6045/2487763522.py in <cell line: 39>()
    336     ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
    337         'tpia_rec',
--> 338         when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (col('TIME_PHYSICAN_INIT_ASSESSMENT').rtrim() == ''), 'B')
    339         .when(col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
    340         .otherwise('Y')

TypeError: 'Column' object is not callable
