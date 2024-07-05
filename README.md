recreate this code maintaining functionality 

# Filter and group the data for numerator
numerator = df_nacrs[index].filter(
    (col('AMCARE_GROUP_CODE') == 'ED') &
    (col('ED_VISIT_IND_CODE') == '1') &
    (col('amcare_type_code') == '11')
).groupBy('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM').agg(count(lit(1)).alias('numerator'))

    # Filter and group the data for denominator
denominator = df_nacrs[index].filter(
    (col('AMCARE_GROUP_CODE') == 'ED') &
    (col('ED_VISIT_IND_CODE') == '1')
).groupBy('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM').agg(count(lit(1)).alias('denominator'))

    # Merge numerator and denominator
ucc = numerator.join(denominator, on=['SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM'], how='inner')

    # Calculate ucc_pct
ucc = ucc.withColumn('ucc_pct', col('numerator') / col('denominator')).orderBy(col('FACILITY_AM_CARE_NUM'))
ucc = ucc.where(ucc['ucc_pct'] >= 0.98)
    # trying code here Filter out rows that are in ucc
    #ucc_filtered = df_list[index].filter(~col('FACILITY_AM_CARE_NUM').isin([row['FACILITY_AM_CARE_NUM'] for row in ucc.select('FACILITY_AM_CARE_NUM').collect()]))
# Append the ucc df to make a list
ucc_all.append(ucc)
ucc_all_corp = reduce(lambda x, y: x.unionAll(y), ucc_all) 
