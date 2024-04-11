# Apply conditions for tpia_org_cmp and tpia_reg_cmp
tpia_org_cmp_a = tpia_org_cmp.withColumn('temp', apply_conditions(F.col('PERCENTILE_90'), F.col('20th_Percentile'), F.col('80th_Percentile'))) \
                            .withColumn('COMPARE_IND_CODE', F.col('temp').getField("COMPARE_IND_CODE")) \
                            .withColumn('COMPARE_IND_E_DESC', F.col('temp').getField("COMPARE_IND_E_DESC")) \
                            .drop('temp') \
                            .orderBy('CORP_ID')

tpia_reg_cmp_a = tpia_reg_cmp.withColumn('temp', apply_conditions(F.col('PERCENTILE_90'), F.col('20th_Percentile'), F.col('80th_Percentile'))) \
                            .withColumn('COMPARE_IND_CODE', F.col('temp').getField("COMPARE_IND_CODE")) \
                            .withColumn('COMPARE_IND_E_DESC', F.col('temp').getField("COMPARE_IND_E_DESC")) \
                            .drop('temp') \
                            .orderBy('REGION_ID')
