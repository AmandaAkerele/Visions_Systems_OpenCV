# Filter los_org_ta DataFrame based on the corrected conditions
los_org_ta = los_org_22.join(ed_nacrs_corp_ids, 'CORP_ID', 'left_anti') \
    .join(los_supp_corp_ids, 'CORP_ID', 'left_anti') \
    .join(ed_facility_filtered_corp_ids, 'CORP_ID', 'left_anti') \
    .filter(
        (col('TYPE') == 'PS') & (col('CORP_CNT') == 1) |
        (col('TYPE') == 'DQ') & (col('CORP_CNT') == 1) & (col('IND') == 'ELOS')
    )

# Remove suppressed corp and non-reported corps for ELOS 
los_corp_conditions = ~los_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) & \
                      ~los_org_22['CORP_ID'].isin(los_supp_org_22['CORP_ID']) & \
                      ~los_org_22['CORP_ID'].isin(
                          ed_facility_org[(ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
                                          ((ed_facility_org['TYPE'] == 'PS') & (ed_facility_org['CORP_CNT'] == 1) |
                                           (ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['CORP_CNT'] == 1) & (ed_facility_org['IND'] == 'ELOS'))]['CORP_ID']
                      )
los_org_ta = los_org_ta[los_corp_conditions]

# Remove Huron Perth Healthcare Alliance, corp_id = 80228 from TPIA and ELOS
tpia_org_ta = tpia_org_ta[(tpia_org_ta['CORP_ID'] != 80228) & tpia_org_ta['CORP_PEER'].notna()]
los_org_ta = los_org_ta[(los_org_ta['CORP_ID'] != 80228) & los_org_ta['CORP_PEER'].notna()]

# Sort DataFrames
tpia_org_ta = tpia_org_ta.sort_values(by='CORP_ID')
los_org_ta = los_org_ta.sort_values(by='CORP_ID')
