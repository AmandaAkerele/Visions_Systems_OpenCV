from pyspark.sql.functions import lit

# Get the list of columns from tmp_ed_facility_org_a
all_columns = tmp_ed_facility_org_a.columns

# Add missing columns as null to t3 and t4
for col in all_columns:
    if col not in t3.columns:
        t3 = t3.withColumn(col, lit(None))
    if col not in t4.columns:
        t4 = t4.withColumn(col, lit(None))


t3 = t3.select(all_columns)
t4 = t4.select(all_columns)

tmp_ed_facility_org = tmp_ed_facility_org_a.union(t3).union(t4).distinct()













from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Inner join
tmp_ed_facility_org_a = df_fac.join(df_dq, (df_fac['FACILITY_AM_CARE_NUM'] == df_dq['FACILITY_AM_CARE_NUM']) & 
                                    (df_fac['SUBMISSION_FISCAL_YEAR'] == df_dq['FISCAL_YEAR']), how='inner')

# Select distinct columns after join to avoid duplicates
tmp_ed_facility_org_a = tmp_ed_facility_org_a.select([F.col('df_fac.' + xx) for xx in df_fac.columns] + 
                                                     [F.col('df_dq.' + yy) for yy in df_dq.columns if yy not in ['FACILITY_AM_CARE_NUM', 'FISCAL_YEAR']])

tmp_ed_facility_org_a = tmp_ed_facility_org_a.withColumn('TYPE', F.lit('DQ'))

# Filter and create t3 DataFrame
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select(['SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM', 'SITE_ID', 'CORP_ID',
                                                        'REGION_ID', 'PROVINCE_ID', 'NACRS_ED_FLG'])
t3 = t3.withColumn('TYPE', F.lit('SL')).withColumn('IND', F.lit(''))

# Filter and create t4 DataFrame
ps = df_ps.filter(df_ps['FISCAL_YEAR'].cast('string') == '2022')
t4 = df_fac.join(ps, df_fac['FACILITY_AM_CARE_NUM'].cast('string') == ps['FACILITY_AM_CARE_NUM'].cast('string'), 'inner')
t4 = t4.withColumn('TYPE', F.lit('PS')).withColumn('IND', F.lit(''))

# Union all DataFrames
tmp_ed_facility_org = tmp_ed_facility_org_a.unionAll(t3).unionAll(t4).distinct()

# Filtering and GroupBy operation
filtered_ed_fac = df_fac.join(tmp_ed_facility_org.select('CORP_ID').distinct(), 'CORP_ID')
tmp_cnt_ed_facility_org = filtered_ed_fac.groupBy('CORP_ID').count().withColumnRenamed('count', 'CORP_CNT')

# Merging DataFrames
ed_facility_org = tmp_ed_facility_org.join(tmp_cnt_ed_facility_org, 'CORP_ID')

# Sorting the DataFrame
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])

# Drop duplicate columns if any
# Note: Spark DataFrames handle duplicate columns differently than Pandas. Typically, duplicate columns are resolved during join operations.
# If you still need to remove duplicate columns, you would have to list out the columns you need explicitly.

# Lastly, remember that PySpark operations are lazy, so actual computation happens when an action (like .show(), .collect()) is called.






or 
from pyspark.sql import functions as F

# Inner join on 'FACILITY_AM_CARE_NUM' and 'SUBMISSION_FISCAL_YEAR' / 'FISCAL_YEAR'
tmp_ed_facility_org_a = df_fac.join(df_dq, (df_fac['FACILITY_AM_CARE_NUM'] == df_dq['FACILITY_AM_CARE_NUM']) &
                                    (df_fac['SUBMISSION_FISCAL_YEAR'] == df_dq['FISCAL_YEAR']), 'inner')


# Selecting columns explicitly to avoid duplicates
selected_columns = [df_fac[col] for col in df_fac.columns] + [df_dq[col] for col in df_dq.columns if col not in ['FACILITY_AM_CARE_NUM', 'FISCAL_YEAR']]
tmp_ed_facility_org_a = tmp_ed_facility_org_a.select(selected_columns)

tmp_ed_facility_org_a = tmp_ed_facility_org_a.withColumn('TYPE', F.lit('DQ'))

t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select(['SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM', 'SITE_ID', 'CORP_ID', 'REGION_ID', 'PROVINCE_ID'])
t3 = t3.withColumn('TYPE', F.lit('SL')).withColumn('IND', F.lit(''))


ps = df_ps.filter(df_ps['FISCAL_YEAR'] == 2022)
t4 = df_fac.join(ps, df_fac['FACILITY_AM_CARE_NUM'] == ps['FACILITY_AM_CARE_NUM'], 'inner')
t4 = t4.withColumn('TYPE', F.lit('PS')).withColumn('IND', F.lit(''))


tmp_ed_facility_org = tmp_ed_facility_org_a.union(t3).union(t4).distinct()


filtered_ed_fac = df_fac.join(tmp_ed_facility_org.select('CORP_ID').distinct(), 'CORP_ID')
tmp_cnt_ed_facility_org = filtered_ed_fac.groupBy('CORP_ID').count().withColumnRenamed('count', 'CORP_CNT')


ed_facility_org = tmp_ed_facility_org.join(tmp_cnt_ed_facility_org, 'CORP_ID')
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])


ed_facility_org = tmp_ed_facility_org.join(tmp_cnt_ed_facility_org, 'CORP_ID')
ed_facility_org = ed_facility_org.orderBy(['TYPE', 'FACILITY_AM_CARE_NUM'])





