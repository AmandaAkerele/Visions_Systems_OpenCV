days_of_the_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

for day in days_of_the_week:
    for hour in range(8, 17):
        print(f"It's {hour}:00 on {day} and I'm at work.")
        if day == 'Friday' and hour == 16:
            print("It's the end of the workday on Friday. Time to go home!")
            break
    if day == 'Friday':
        break



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




orrrrr


from pyspark.sql.functions import col

# Filter ed_facility_org for specific conditions and alias the result
ed_facility_org_filtered = ed_facility_org_alias.filter(
    (col("edfo.TYPE") == 'SL') & (col("edfo.CORP_CNT") == 1)
).select("edfo.CORP_ID")

# Perform the join operation
ed_nacrs_flg_1_22 = df_fac_alias.join(
    ed_facility_org_filtered, 
    df_fac_alias["CORP_ID"] == ed_facility_org_filtered["CORP_ID"]
).filter(df_fac_alias["NACRS_ED_FLG"] == 1)

# Remove suppressed corp and non-reported corps
tpia_corp_conditions = (
    ~tpia_org_22_alias['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) &
    ~tpia_org_22_alias['CORP_ID'].isin(tpia_supp_org_alias['CORP_ID']) &
    ~tpia_org_22_alias['CORP_ID'].isin(
        ed_facility_org_alias.filter(
            (col("edfo.SUBMISSION_FISCAL_YEAR") == "2022") &
            (
                (col("edfo.TYPE") == 'PS') & (col("edfo.CORP_CNT") == 1) |
                (col("edfo.TYPE") == 'DQ') & (col("edfo.CORP_CNT") == 1) & (col("edfo.IND") == 'TPIA')
            )
        ).select("edfo.CORP_ID")
    )
)

# Apply the filter conditions
tpia_org_ta = tpia_org_22_alias.filter(tpia_corp_conditions)

# Remove suppressed corp and non-reported corps for ELOS 
los_corp_conditions = (
    ~los_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22['CORP_ID']) &
    ~los_org_22['CORP_ID'].isin(los_supp_org_22['CORP_ID']) &
    ~los_org_22['CORP_ID'].isin(
        ed_facility_org_alias.filter(
            (col("edfo.SUBMISSION_FISCAL_YEAR") == "2022") &
            (
                (col("edfo.TYPE") == 'PS') & (col("edfo.CORP_CNT") == 1) |
                (col("edfo.TYPE") == 'DQ') & (col("edfo.CORP_CNT") == 1) & (col("edfo.IND") == 'ELOS')
            )
        ).select("edfo.CORP_ID")
    )
)

los_org_ta = los_org_22[los_corp_conditions]

# Remove Huron Perth Healthcare Alliance, corp_id = 80228 from TPIA and ELOS
tpia_org_ta = tpia_org_ta.filter((tpia_org_ta['CORP_ID'] != 80228) & tpia_org_ta['CORP_PEER'].isNotNull())
los_org_ta = los_org_ta.filter((los_org_ta['CORP_ID'] != 80228) & los_org_ta['CORP_PEER'].isNotNull())

# Sort DataFrames
tpia_org_ta = tpia_org_ta.orderBy('CORP_ID')
los_org_ta = los_org_ta.orderBy('CORP_ID')


