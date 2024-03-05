# Create multiple_sites DataFrame
multiple_sites = df_fac.join(stand_alone, 'CORP_ID', 'inner') \
    .groupBy('CORP_ID').agg(countDistinct('FAC_ID').alias('cnt_sites')) \
    .filter(col('cnt_sites') > 1)
