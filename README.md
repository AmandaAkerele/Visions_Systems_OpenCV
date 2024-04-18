# Organization 90th percentile exclude <5 and DQ
los_org_22 = ed_record_admit_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID', 'CORP_NAME','CORP_PEER') \
                          .agg(F.percentile_approx('LOS_HOURS', 0.9).alias('PERCENTILE_90'))

# Filter rows based on specific 'SITE_ID' values
filtered_rows_los = los_site_22.filter(col('SITE_ID').isin([5096, 5099, 5103, 5209]))

# Select and rename specific columns
filtered_rows_los = filtered_rows_los.select(
    col('SUBMISSION_FISCAL_YEAR'),
    col('SITE_ID').alias('CORP_ID'),
    col('SITE_NAME').alias('CORP_NAME'),
    col('SITE_PEER').alias('CORP_PEER'),
    col('Percentile_90')
)

# Union the datasets using unionByName with allowMissingColumns=True
los_org_22 = los_org_22.unionByName(filtered_rows_los, allowMissingColumns=True)

los_org_22.show()
los_org_22.shape()
