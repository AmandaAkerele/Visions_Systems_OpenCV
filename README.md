
# # Create standalone DataFrame
stand_alone = df_fac.filter(col('NACRS_ED_FLG') == 1).select('CORP_ID').distinct()

# Create multiple_sites DataFrame
multiple_sites = df_fac.join(stand_alone, 'CORP_ID')
multiple_sites = multiple_sites.groupBy('CORP_ID').agg(count('*').alias('cnt_sites'))
multiple_sites = multiple_sites.filter(col('cnt_sites') > 1)
