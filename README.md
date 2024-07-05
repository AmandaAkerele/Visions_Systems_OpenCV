# Filter and group the data for numerator and denominator
filtered_df = df_nacrs[index].filter(
    (col('AMCARE_GROUP_CODE') == 'ED') & 
    (col('ED_VISIT_IND_CODE') == '1')
)

# Numerator: Specific amcare_type_code '11'
numerator = filtered_df.filter(
    col('amcare_type_code') == '11'
).groupBy('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM').agg(count(lit(1)).alias('numerator'))

# Denominator: All relevant records
denominator = filtered_df.groupBy('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM').agg(count(lit(1)).alias('denominator'))

# Merge numerator and denominator and calculate ucc_pct
ucc = numerator.join(denominator, on=['SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM'])
ucc = ucc.withColumn('ucc_pct', col('numerator') / col('denominator')).where(col('ucc_pct') >= 0.98)

# Append the ucc df to make a list
ucc_all.append(ucc)
ucc_all_corp = reduce(lambda x, y: x.unionAll(y), ucc_all)

# Filter out rows that are in ucc using anti-join
ucc_filtered = df_nacrs[index].join(ucc.select('FACILITY_AM_CARE_NUM').distinct(), on='FACILITY_AM_CARE_NUM', how='left_anti')
