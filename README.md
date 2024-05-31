from pyspark.sql import functions as F

# Filter the DataFrame to include only rows where VISIT_DISPOSITION is '06' or '07'
ed_records_admit_with_ucc_22_bb_df = ed_record_with_ucc_22_aa.filter(
    F.col('VISIT_DISPOSITION').isin(['06', '07'])
)

# Show the resulting DataFrame
ed_records_admit_with_ucc_22_bb_df.show()
