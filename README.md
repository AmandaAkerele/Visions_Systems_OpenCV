firstly create a NACRS_ED_FLG in df_fac based on the below 

#prepare data for stand alones
alone = spark.createDataFrame(
    [
        (99012,29061,1),
        (80335,48006,1),
        (80335,48008,1),
        (80337,48015,1),
        (80337,48022,1),
        (80337,48023,1),
        (80337,48024,1),
        (80345,48029,1),
        (80338,48032,1),
        (80339,48037,1),
        (80340,48039,1),
        (80344,48044,1),
        (80341,48053,1),
        (80345,48063,1),
        (80347,48076,1),
        (80348,48083,1),
        (80348,48085,1),
        (80348,48086,1),
        (80338,48116,1),
        (80347,48117,1),
        (80337,48120,1),
        (80338,48121,1),
        (5160, 54242,1),
        (7043,71117,1),
        (7070,71163,1),
        (973,88050,1),
        (1006,88080,1),
        (986,88132,1),
        (20390,88142,1),
        (99718,88149,1),
        (99724,88155,1),
        (20282,88349,1),
        (20400,88350,1),
        (99725,88391,1),
        (99726,88394,1),
        (80226,88578,1),
        (80517,88595,1),
        (99768,88922,1
)
    ],
    ['CORP_ID','FACILITY_AM_CARE_NUM', 'NACRS_ED_FLG']  
)

alone.printSchema()


alone.show()

# Create DataFrames t3 and t4 based on conditions
t3 = df_fac[df_fac['NACRS_ED_FLG'] == 1][columns_to_keep].copy()
t3['TYPE'] = 'SL'
t3['IND'] = ''
