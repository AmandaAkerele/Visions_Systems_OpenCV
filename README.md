# Filter df_fac based on org_id present in ed_nodup_nosb_22
df_fac = df_org_dim.join(ed_nodup_nosb_22.select('org_id').distinct(), 'org_id')

# Prepare data for stand alones
SL = spark.createDataFrame(
    [
        (99012, 29061, 1), (80335, 48006, 1), (80335, 48008, 1), (80337, 48015, 1), 
        (80337, 48022, 1), (80337, 48023, 1), (80337, 48024, 1), (80345, 48029, 1), 
        (80338, 48032, 1), (80339, 48037, 1), (80340, 48039, 1), (80344, 48044, 1), 
        (80341, 48053, 1), (80345, 48063, 1), (80347, 48076, 1), (80348, 48083, 1), 
        (80348, 48085, 1), (80348, 48086, 1), (80338, 48116, 1), (80347, 48117, 1), 
        (80337, 48120, 1), (80338, 48121, 1), (5160, 54242, 1), (7043, 71117, 1), 
        (7070, 71163, 1), (973, 88050, 1), (1006, 88080, 1), (986, 88132, 1), 
        (20390, 88142, 1), (99718, 88149, 1), (99724, 88155, 1), (20282, 88349, 1), 
        (20400, 88350, 1), (99725, 88391, 1), (99726, 88394, 1), (80226, 88578, 1), 
        (80517, 88595, 1), (99768, 88922, 1)
    ],
    ['CORP_ID', 'FACILITY_AM_CARE_NUM', 'NACRS_ED_FLG']
)

# Join df_fac with SL to add NACRS_ED_FLG column
df_fac1 = df_fac.join(SL, (df_fac.corp_id == SL.CORP_ID) & (df_fac.FACILITY_AM_CARE_NUM == SL.FACILITY_AM_CARE_NUM), how='left') \
               .select(df_fac["*"], SL["NACRS_ED_FLG"])

# Fill null NACRS_ED_FLG values with 0
df_fac1 = df_fac1.fillna({'NACRS_ED_FLG': 0})

# Show the final DataFrame df_fac1
df_fac1.show()
