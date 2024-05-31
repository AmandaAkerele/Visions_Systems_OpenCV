ed_records_22_bb_df = ed_nodup_noucc_nosb_22.filter(~col('VISIT_DISPOSITION').isin(['61', '63']))

solve error below 


ed_records_22_bb_df = ed_nodup_noucc_nosb_22.filter(~col('VISIT_DISPOSITION').isin(['61', '63']))
---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
/tmp/ipykernel_290/3641738683.py in <cell line: 1>()
----> 1 ed_records_22_bb_df = ed_nodup_noucc_nosb_22.filter(~col('VISIT_DISPOSITION').isin(['61', '63']))

TypeError: 'str' object is not callable
