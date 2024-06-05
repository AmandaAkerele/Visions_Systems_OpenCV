using this method 

# For peer group remove UCC and standalones only
ed_record_22_Peer= ed_records_22_bb_df.filter(~col('FACILITY_AM_CARE_NUM').isin([row['FACILITY_AM_CARE_NUM'] for row in SL.select('FACILITY_AM_CARE_NUM').collect()]))
#

calculate the ed_record_22_Peer and factor in the:  != '54242')] in the code below to the code above
# Create ed_record 22_Peer
ed_record_22_Peer = ed_records_22_bb_df[(
    ~ed_records_22_bb_df['FACILITY_AM_CARE_NUM'].isin(ed_facility_org[(ed_facility_org['TYPE'] == 'SL') & (ed_facility_org['FACILITY_AM_CARE_NUM'] != '54242')]['FACILITY_AM_CARE_NUM'])
    & ~ed_records_22_bb_df['FACILITY_AM_CARE_NUM'].isin(ed_facility_org[(ed_facility_org['TYPE']== 'DQ') & (ed_facility_org['IND'] == 'TPIA')]['FACILITY_AM_CARE_NUM'])
)]

