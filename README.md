from pyspark.sql import functions as F

# Apply conditions to create the 'tpia_rec' column
ed_record = ed_record.withColumn(
    'tpia_rec',
    F.when(
        F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B'
    ).when(
        F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N'
    ).otherwise('Y')
)

# Filter out records where 'tpia_rec' is not 'B'
tpia_org_cnt = ed_record.filter(F.col('tpia_rec') != 'B')

# Group by and aggregate data
tpia_org_rec = tpia_org_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn(
    'tpia_rec_pct',
    F.col('tpia_calc_cnt') / F.col('Total_CASE')
)

# Filter DataFrame based on specific conditions for suppression and reporting
tpia_supp_org = tpia_org_rec.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)

tpia_rpt_org = tpia_org_rec.filter(
    (F.col('tpia_calc_cnt') >= 50) | ((F.col('tpia_calc_cnt') < 50) & (F.col('tpia_rec_pct') >= 0.75))
)

# Create 'tpia_rec' column for ed_record_with_ucc_22
ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn('tpia_rec',
    F.when(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] == '9999', 'N')
    .when(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'].isNotNull() & 
          (ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] != '9999'), 'Y')
    .otherwise('B')
)

# Filter out rows with 'tpia_rec' equal to 'B'
tpia_org_cnt_ucc_22 = ed_record_with_ucc_22.filter(ed_record_with_ucc_22['tpia_rec'] != 'B')

# Aggregation
tpia_org_rec_ucc_22 = tpia_org_cnt_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
    F.count('AM_CARE_KEY').alias('Total_CASE'),
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn('tpia_rec_pct', F.col('tpia_calc_cnt') / F.col('Total_CASE'))

# Filter 'tpia_org_rec_ucc' based on conditions
tpia_supp_org_ucc_22 = tpia_org_rec_ucc_22.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)
tpia_rpt_org_ucc_22 = tpia_org_rec_ucc_22.filter(
    (F.col('tpia_calc_cnt') >= 50) | ((F.col('tpia_calc_cnt') < 50) & (F.col('tpia_rec_pct') >= 0.75))
)

# Append the ucc df to make a list
tpia_supp_org_all.append(tpia_supp_org_ucc_22)

# Stack all processed ucc using reduce and unionAll()
tpia_supp_corp_combined = reduce(lambda x, y: x.unionAll(y), tpia_supp_org_all)
