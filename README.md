# Aggregating LOS_org_cnt
tpia_org_cnt2 = ed_record_admit_noucc.groupby('SUBMISSION_FISCAL_YEAR', 'CORP_ID') \
    .agg(sum(when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNotNull(), 1)).alias('tpia_calc_cnt')
# Conditional DataFrame creation
tpia_supp_org2 = tpia_org_cnt2.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
tpia_rpt_org2 = tpia_org_cnt2.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))

solve error below 
File "/tmp/ipykernel_7001/2044627681.py", line 3
    .agg(sum(when(col('TIME_PHYSICAN_INIT_ASSESSMENT').isNotNull(), 1)).alias('tpia_calc_cnt')
        ^
SyntaxError: '(' was never closed
