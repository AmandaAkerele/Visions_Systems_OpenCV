from pyspark.sql import functions as F

# Group by 'CORP_ID' and count 'AM_CARE_KEY', then rename the column to 'ED_CNT'
ED_CORP_22_df = ed_nodup_noucc_nosb_22.groupBy('CORP_ID').agg(F.count('AM_CARE_KEY').alias('ED_CNT'))

# Show the resulting DataFrame
ED_CORP_22_df.show()
