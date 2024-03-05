# # Create standalone DataFrame
stand_alone = df_fac.filter(col('NACRS_ED_FLG') == 1).select('CORP_ID').distinct()
