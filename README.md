from pyspark.sql.functions import col

# Collect facility numbers to exclude based on 'TYPE' and 'IND'
exclude_facility_nums_for_TPIA = ed_facility_org.filter(
    (col('TYPE') == 'DQ') & (col('IND') == 'TPIA')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

exclude_types = ['SL', 'PS', 'DQ']
exclude_facility_nums = ed_facility_org.filter(
    col('TYPE').isin(exclude_types)
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

# Filter ed_record_bb DataFrame
ed_record = ed_record_bb.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums) &
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_for_TPIA)
)

# For PEER group, remove 'SL' and specific facility numbers
exclude_facility_nums_sl = ed_facility_org.filter(
    (col('TYPE') == 'SL') & (col('FACILITY_AM_CARE_NUM') != '54242')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

exclude_facility_nums_dq = ed_facility_org.filter(
    (col('TYPE') == 'DQ') & (col('IND') == 'TPIA')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

# Create ed_record_Peer DataFrame
ed_record_Peer = ed_record_bb.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_sl) &
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_dq)
)

# Filter for ed_record_with_ucc dataset
exclude_facility_nums_tpia = ed_facility_org.filter(
    (col('TYPE') == 'DQ') & (col('IND') == 'TPIA')
).select('FACILITY_AM_CARE_NUM').distinct().rdd.flatMap(lambda x: x).collect()

# Create ed_record_with_ucc DataFrame TPIA
ed_record_with_ucc = ed_record_with_ucc_bb.filter(
    ~col('FACILITY_AM_CARE_NUM').isin(exclude_facility_nums_tpia)
)
