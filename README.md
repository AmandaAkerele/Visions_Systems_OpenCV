from pyspark.sql.functions import col

# Filter out records where FACILITY_AM_CARE_NUM is not '54242'
filtered_records = ed_records_22_bb_df.filter(col('FACILITY_AM_CARE_NUM') != '54242')

# Get a list of FACILITY_AM_CARE_NUM from SL DataFrame to exclude (for UCC and standalone)
exclusion_list = [row['FACILITY_AM_CARE_NUM'] for row in SL.select('FACILITY_AM_CARE_NUM').collect()]

# Create a peer group by excluding the records with FACILITY_AM_CARE_NUM in the exclusion list
ed_record_22_Peer = filtered_records.filter(~col('FACILITY_AM_CARE_NUM').isin(exclusion_list))
