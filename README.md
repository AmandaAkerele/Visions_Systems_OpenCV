from pyspark.sql import functions as F

# Create 'tpia_rec' column with initial value 'Y'
ed_record = ed_record.withColumn('tpia_rec', F.lit('Y'))

# Update 'tpia_rec' based on conditions
ed_record = ed_record.withColumn('tpia_rec', 
                                 F.when(ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'].isNull() | 
                                        (ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'] == ''), 'B')
                                  .when(ed_record['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'N')
                                  .otherwise(ed_record['tpia_rec']))

# Filter records not equal to 'B'
tpia_org_cnt = ed_record.filter(ed_record['tpia_rec'] != 'B')

# Aggregation
tpia_org_rec = tpia_org_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
)

# Calculate percentage and sort
tpia_org_rec = tpia_org_rec.withColumn('tpia_rec_pct', col('tpia_calc_cnt') / col('Total_CASE'))
tpia_org_rec = tpia_org_rec.orderBy('CORP_ID')

# Conditional DataFrame creation
tpia_supp_org = tpia_org_rec.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
tpia_rpt_org = tpia_org_rec.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))



# Create 'tpia_rec' column
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn('tpia_rec', 
                                                         F.when(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'N')
                                                          .when(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'].isNotNull() & 
                                                                (ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] != '9999'), 'Y')
                                                          .otherwise('B'))

# Filter out rows with 'tpia_rec' equal to "B"
tpia_org_cnt_ucc_22 = ed_record_with_ucc_22.filter(ed_record_with_ucc_22['tpia_rec'] != 'B')

# Aggregation
tpia_org_rec_ucc_22 = tpia_org_cnt_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn('tpia_rec_pct', col('tpia_calc_cnt') / col('Total_CASE'))

# Filter 'tpia_org_rec_ucc' based on conditions
tpia_supp_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
tpia_rpt_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))





