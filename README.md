exclude_types = ['SL', 'PS', 'DQ']
exclude_facility_nums_for_TPIA = ed_facility_org[ed_facility_org['TYPE'].isin(['DQ']) & (ed_facility_org['IND'] == 'TPIA')]['FACILITY_AM_CARE_NUM']
exclude_facility_nums = ed_facility_org[ed_facility_org['TYPE'].isin(exclude_types)]['FACILITY_AM_CARE_NUM']
# Filter ed_record_bb DataFrame
ed_record = ed_record_bb[~ed_record_bb['FACILITY_AM_CARE_NUM'].isin(exclude_facility_nums) & ~ed_record_bb['FACILITY_AM_CARE_NUM'].isin(exclude_facility_nums_for_TPIA)]

#For PEER group remove ucc and standalones only;
# Filter out specific rows from ed_facility_org
exclude_facility_nums_sl = ed_facility_org[(ed_facility_org['TYPE'] == 'SL') & (ed_facility_org['FACILITY_AM_CARE_NUM'] != '54242')]['FACILITY_AM_CARE_NUM']
exclude_facility_nums_dq = ed_facility_org[(ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['IND'] == 'TPIA')]['FACILITY_AM_CARE_NUM']

# Create ed_record_Peer DataFrame
ed_record_Peer = ed_record_bb[~ed_record_bb['FACILITY_AM_CARE_NUM'].isin(exclude_facility_nums_sl) &
                                    ~ed_record_bb['FACILITY_AM_CARE_NUM'].isin(exclude_facility_nums_dq)]

# Filter out specific rows from ed_facility_org for ed_record_with_ucc
# dataset with UCC for NAT, PROV, REG level
#remove DQ using datasets with UCC
exclude_facility_nums_tpia = ed_facility_org[(ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['IND'] == 'TPIA')]['FACILITY_AM_CARE_NUM']

# Create ed_record_with_ucc DataFrame TPIA
ed_record_with_ucc = ed_record_with_ucc_bb[~ed_record_with_ucc_bb['FACILITY_AM_CARE_NUM'].isin(exclude_facility_nums_tpia)]

