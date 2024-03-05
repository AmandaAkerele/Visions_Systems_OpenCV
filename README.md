# Sort the resulting DataFrame by CORP_ID
ed_nacrs_flg_1_22.sort_values(by ='CORP_ID', inplace=True)

# Remove suppressed corp and non-reported corps
tpia_corp_conditions = ~tpia_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) & \
                       ~tpia_org_22['CORP_ID'].isin(tpia_supp_org['CORP_ID']) & \
                       ~tpia_org_22['CORP_ID'].isin(
                           ed_facility_org[(ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
                                           ((ed_facility_org['TYPE'] == 'PS') & (ed_facility_org['CORP_CNT'] == 1) |
                                            (ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['CORP_CNT'] == 1) & (ed_facility_org['IND'] == 'TPIA'))]['CORP_ID']
                       )
tpia_org_ta = tpia_org_22[tpia_corp_conditions]

# Remove suppressed corp and non-reported corps for ELOS 
los_corp_conditions = ~los_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) & \
                      ~los_org_22['CORP_ID'].isin(los_supp_org_22['CORP_ID']) & \
                      ~los_org_22['CORP_ID'].isin(
                          ed_facility_org[(ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
                                          ((ed_facility_org['TYPE'] == 'PS') & (ed_facility_org['CORP_CNT'] == 1) |
                                           (ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['CORP_CNT'] == 1) & (ed_facility_org['IND'] == 'ELOS'))]['CORP_ID']
                      )
los_org_ta = los_org_22[los_corp_conditions]

# Remove Huron Perth Healthcare Alliance, corp_id = 80228 from TPIA and ELOS
tpia_org_ta = tpia_org_ta[(tpia_org_ta['CORP_ID'] != 80228) & tpia_org_ta['CORP_PEER'].notna()]
los_org_ta = los_org_ta[(los_org_ta['CORP_ID'] != 80228) & los_org_ta['CORP_PEER'].notna()]

# Sort DataFrames
tpia_org_ta = tpia_org_ta.sort_values(by='CORP_ID')
los_org_ta = los_org_ta.sort_values(by='CORP_ID')

# Sort the resulting DataFrame by CORP_ID
ed_nacrs_flg_1_22.sort_values(by ='CORP_ID', inplace=True)

# Remove suppressed corp and non-reported corps
tpia_corp_conditions = ~tpia_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) & \
                       ~tpia_org_22['CORP_ID'].isin(tpia_supp_org['CORP_ID']) & \
                       ~tpia_org_22['CORP_ID'].isin(
                           ed_facility_org[(ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
                                           ((ed_facility_org['TYPE'] == 'PS') & (ed_facility_org['CORP_CNT'] == 1) |
                                            (ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['CORP_CNT'] == 1) & (ed_facility_org['IND'] == 'TPIA'))]['CORP_ID']
                       )
tpia_org_ta = tpia_org_22[tpia_corp_conditions]

# Remove suppressed corp and non-reported corps for ELOS 
los_corp_conditions = ~los_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) & \
                      ~los_org_22['CORP_ID'].isin(los_supp_org_22['CORP_ID']) & \
                      ~los_org_22['CORP_ID'].isin(
                          ed_facility_org[(ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
                                          ((ed_facility_org['TYPE'] == 'PS') & (ed_facility_org['CORP_CNT'] == 1) |
                                           (ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['CORP_CNT'] == 1) & (ed_facility_org['IND'] == 'ELOS'))]['CORP_ID']
                      )
los_org_ta = los_org_22[los_corp_conditions]

# Remove Huron Perth Healthcare Alliance, corp_id = 80228 from TPIA and ELOS
tpia_org_ta = tpia_org_ta[(tpia_org_ta['CORP_ID'] != 80228) & tpia_org_ta['CORP_PEER'].notna()]
los_org_ta = los_org_ta[(los_org_ta['CORP_ID'] != 80228) & los_org_ta['CORP_PEER'].notna()]

# Sort DataFrames
tpia_org_ta = tpia_org_ta.sort_values(by='CORP_ID')
los_org_ta = los_org_ta.sort_values(by='CORP_ID')

# Calculate percentile for TPIA
prct_20_80_tpia_org_22_ta = tpia_org_ta.groupby('CORP_PEER')['PERCENTILE_90'].quantile([0.2, 0.8]).unstack()

# Calculate percentile for ELOS
prct_20_80_los_org_22_ta= los_org_ta.groupby('CORP_PEER')['PERCENTILE_90'].quantile([0.2, 0.8]).unstack()


# *Regional Level Baseline - National: Since the regional values contain all UCC and stand-alone then the national one should include UCC and stand-alone;
# *Update on baselines: 20% tile and 80%tile of national regional value;

# Calculate percentile for national regional values for TPIA
prct_20_80_tpia_reg_22_ta= tpia_reg_22_ta.groupby('SUBMISSION_FISCAL_YEAR')['PERCENTILE_90'].quantile([0.2, 0.8]).unstack().reset_index()

# Calculate percentile for national regional values for ELOS
prct_20_80_los_reg_22_ta= los_reg_22_ta.groupby('SUBMISSION_FISCAL_YEAR')['PERCENTILE_90'].quantile([0.2, 0.8]).unstack().reset_index()

