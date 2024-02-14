from pyspark.sql import functions as F

# Assuming df_fac and df_dq are your Spark DataFrames and already loaded

# Inner join df_fac and df_dq while avoiding duplicate columns
tmp_ed_facility_org_a = df_fac.join(df_dq, 
    (df_fac['FACILITY_AM_CARE_NUM'] == df_dq['FACILITY_AM_CARE_NUM']) & 
    (df_fac['SUBMISSION_FISCAL_YEAR'] == df_dq['FISCAL_YEAR']), 
    'inner')

# Drop duplicated columns from df_dq
duplicate_columns = ['SITE_ID', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID']
for column in duplicate_columns:
    tmp_ed_facility_org_a = tmp_ed_facility_org_a.drop(df_dq[column])

# Add TYPE column
tmp_ed_facility_org_a = tmp_ed_facility_org_a.withColumn('TYPE', F.lit('DQ'))

# Filter df_fac and create t3 DataFrame
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM', 
                                                       'SITE_ID', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID', 
                                                       'NACRS_ED_FLG')
t3 = t3.withColumn('TYPE', F.lit('SL')).withColumn('IND', F.lit(''))

# Filter df_ps and create t4 DataFrame
ps = df_ps.filter(df_ps['FISCAL_YEAR'] == '2022')
t4 = df_fac.join(ps, df_fac['FACILITY_AM_CARE_NUM'] == ps['FACILITY_AM_CARE_NUM'], 'inner')
t4 = t4.withColumn('TYPE', F.lit('PS')).withColumn('IND', F.lit(''))

# Union DataFrames
tmp_ed_facility_org = tmp_ed_facility_org_a.union(t3).union(t4).dropDuplicates()

# Filter and group by
filtered_ed_fac = df_fac.join(tmp_ed_facility_org, 'CORP_ID')
tmp_cnt_ed_facility_org = filtered_ed_fac.groupBy('CORP_ID').count().withColumnRenamed('count', 'CORP_CNT')

# Merge and sort
ed_facility_org = tmp_ed_facility_org.join(tmp_cnt_ed_facility_org, 'CORP_ID').orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Drop duplicates if any
ed_facility_org = ed_facility_org.dropDuplicates()
