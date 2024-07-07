solve error in the code below 

environment ="prod"

# Get current date
current_date = datetime.now()

# Determine the comparison date as March 31 of the current year
comparison_date = datetime(current_date.year, 3, 31)

# Determine the right closed year based on the comparison date
if current_date > comparison_date:
    closed_year = current_date.year - 1
else:
    closed_year = current_date.year - 2

# List to store the results for each year
yr_list = [str(closed_year-4), str(closed_year-3), str(closed_year-2), str(closed_year-1), str(closed_year)]
df_list = [None] * 5  # Initialize list with None values

ucc_data = []
stacked_all = []
tpia_corp_all =[]
tpia_reg_all=[]
tpia_supp_orgs = []
tpia_supp_regs =[]
tpia_supp_peers =[]
ps_all = []
ucc_all=[]
test=[]


oi = read_redshift_into_df(
        spark=spark,
        rs_username=redshift_username,
        rs_password=redshift_password,
        schema="org_info",
        table="org_dim",
        #key_col=key_col,
        env=environment
        #where=where,
        #columns=columns,
        #limit=limit,
        #trim=trim
        )
    #oi.show(n=10)
#prepare data for stand alones
alone = spark.createDataFrame(
    [
        (99012,29061,1),
        (80335,48006,2),
        (80335,48008,2),
        (80337,48015,5),
        (80337,48022,5),
        (80337,48023,5),
        (80337,48024,5),
        (80345,48029,2),
        (80338,48032,3),
        (80339,48037,1),
        (80340,48039,1),
        (80344,48044,1),
        (80341,48053,1),
        (80345,48063,2),
        (80347,48076,2),
        (80348,48083,3),
        (80348,48085,3),
        (80348,48086,3),
        (80338,48116,3),
        (80347,48117,2),
        (80337,48120,5),
        (80338,48121,3),      
        (7043,71117,1),
        (7070,71163,1),
        (973,88050,1),
        (5160,54242,1),
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
    ['CORP_ID','FACILITY_AM_CARE_NUM', 'CNT_SITE']  # add your column names here
)
alone_a=alone.filter(~col('FACILITY_AM_CARE_NUM').isin(['54242']))


# Loop through the last five years
for index in range(0, 5):
    df_list[index] = read_redshift_into_df(
        spark=spark,
        rs_username=redshift_username,
        rs_password=redshift_password,
        schema="nacrs_enrich_wide",
        table="nacrs_encounter_wide",
        columns=[ 'org_id','AM_CARE_KEY', 'SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'facility_site_name', 'AMCARE_GROUP_CODE',
    'FACILITY_AM_CARE_NUM', 'TRIAGE_DATE', 'TRIAGE_TIME', 'DATE_OF_REGISTRATION', 'REGISTRATION_TIME', 'amcare_type_code',
    'DISPOSITION_DATE', 'DISPOSITION_TIME', 'VISIT_DISPOSITION', 'WAIT_TIME_TO_PIA_HOURS',
    'LOS_HOURS', 'WAIT_TIME_TO_INPATIENT_HOURS', 'TIME_PHYSICAN_INIT_ASSESSMENT', 'acute_care_number',
    'ED_VISIT_IND_CODE', 'GENDER', 'AGE_NUM', 'facility_name', 'facility_postal_code', 'submitting_facility_province',
    'PATIENT_CIHI_REGION_CODE','PATIENT_CIHI_REGION_E_DESC', 'problem_01',	 'problem_02',	 'problem_03',	 'problem_04',
           'problem_05',	 'problem_06',	 'problem_07',	 'problem_08',	 'problem_09',	 'problem_10', 'submission_period'],
        where="WHERE fiscal_year = " + yr_list[index],
        env=environment
    )
    
        # Filter and group the data for numerator
    numerator = df_list[index].filter(
        (col('AMCARE_GROUP_CODE') == 'ED') &
        (col('ED_VISIT_IND_CODE') == '1') &
        (col('amcare_type_code') == '11')
    ).groupBy('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM').agg(count(lit(1)).alias('numerator'))
    
        # Filter and group the data for denominator
    denominator = df_list[index].filter(
        (col('AMCARE_GROUP_CODE') == 'ED') &
        (col('ED_VISIT_IND_CODE') == '1')
    ).groupBy('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM').agg(count(lit(1)).alias('denominator'))
    
        # Merge numerator and denominator
    ucc = numerator.join(denominator, on=['SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM'], how='inner')
    
        # Calculate ucc_pct
    ucc = ucc.withColumn('ucc_pct', col('numerator') / col('denominator')).orderBy(col('FACILITY_AM_CARE_NUM'))
    ucc = ucc.where(ucc['ucc_pct'] >= 0.98)
        # trying code here Filter out rows that are in ucc
        #ucc_filtered = df_list[index].filter(~col('FACILITY_AM_CARE_NUM').isin([row['FACILITY_AM_CARE_NUM'] for row in ucc.select('FACILITY_AM_CARE_NUM').collect()]))
    # Append the ucc df to make a list
    ucc_all.append(ucc)
    ucc_all_corp = reduce(lambda x, y: x.unionAll(y), ucc_all) 

    
    
    #this code is to find out the partial submission facilities ...
    b = df_list[index].filter(df_list[index]['amcare_type_code'].isin(['10'])).groupby('SUBMISSION_FISCAL_YEAR','org_id','FACILITY_PROVINCE','facility_am_care_num', 'submission_period')\
                              .agg(count("*").alias("total_count")).orderBy('SUBMISSION_FISCAL_YEAR','org_id','facility_am_care_num', 'submission_period')
    b = b.withColumn("submission_period", col("submission_period").cast("int"))
    
    QC_BC=b.filter(col('FACILITY_PROVINCE').isin("QC", "BC")) 
    ALL = b.filter((~col('FACILITY_PROVINCE').isin("QC", "BC")) & (col('submission_period') < 13))
    all_facilities = ALL.select("SUBMISSION_FISCAL_YEAR","org_id","facility_am_care_num").distinct()
    QC_BC_facilities=QC_BC.select("SUBMISSION_FISCAL_YEAR","org_id","facility_am_care_num").distinct()
    all_periods = spark.range(1, 13).select(col("id").alias("submission_period"))
    QC_BC_periods=spark.range(1, 14).select(col("id").alias("submission_period"))
    all_combinations = all_facilities.crossJoin(all_periods)
    #all_combinations.show()
    QCBC_combinations = QC_BC_facilities.crossJoin(QC_BC_periods)
    #QCBC_combinations.show()
    result_df1 = all_combinations.join(ALL, ['SUBMISSION_FISCAL_YEAR','org_id',"facility_am_care_num", "submission_period"], "left_outer")\
    .orderBy('SUBMISSION_FISCAL_YEAR','org_id','facility_am_care_num', 'submission_period')
    result_df2 = QCBC_combinations.join(QC_BC, ['SUBMISSION_FISCAL_YEAR','org_id',"facility_am_care_num", "submission_period"], "left_outer")\
    .orderBy('SUBMISSION_FISCAL_YEAR','org_id','facility_am_care_num', 'submission_period')
    #result_df2.show(n=20)
    # Filter out periods with total_count of 0
    periods_with_zero1 = result_df1.filter(col("total_count").isNull()) .groupBy("SUBMISSION_FISCAL_YEAR","org_id","facility_am_care_num").agg(count("*").alias("count_periods_with_zero"))
    #periods_with_zero1.show()
    periods_with_zero2 = result_df2.filter(col("total_count").isNull()) .groupBy("SUBMISSION_FISCAL_YEAR","org_id","facility_am_care_num").agg(count("*").alias("count_periods_with_zero"))
    #periods_with_zero2.show()
    periods_with_zero=periods_with_zero1.union(periods_with_zero2)
    # Filter facilities where there are two or more periods with total_count of 0
    ps_a = periods_with_zero.filter(col("count_periods_with_zero") >= 2)
    ps = ps_a.filter(~col('FACILITY_AM_CARE_NUM').isin([row['FACILITY_AM_CARE_NUM'] for row in ucc.select('FACILITY_AM_CARE_NUM').collect()]))
    #ps.show()
    ps_all.append(ps)
    ps_all_corp = reduce(lambda x, y: x.unionAll(y), ps_all)
    
    
    ed_nodup_nosb_b = df_list[index].filter((col('ED_VISIT_IND_CODE') == '1') &(col('AMCARE_GROUP_CODE') == 'ED'))
    #remove still births
    diagnosis_codes = ['P95']
    max_dx_columns = 10
    
    # Create a list of column conditions to check each column for the diagnosis codes
    conditions = [col(f"problem_{str(i).zfill(2)}").startswith(code) 
                  for i in range(1, max_dx_columns + 1) 
                  for code in diagnosis_codes]
    
    # Combine the conditions using OR logic
    combined_condition = conditions[0]
    for condition in conditions[1:]:
        combined_condition |= condition
    
    # Add the `Flag` column to the DataFrame
    sb_a = ed_nodup_nosb_b.withColumn("Flag", when(combined_condition, lit(1)).otherwise(lit(0)))
    
    
    
    # Filter out rows where `Flag` is 1
    sb = sb_a.filter(col("Flag") == 1)
    ed_nodup_nosb = ed_nodup_nosb_b.join(sb, ed_nodup_nosb_b.am_care_key == sb.am_care_key, "leftanti")
    oi_alias = oi.alias("oi") 
    ed_nodup_nosb_aa = ed_nodup_nosb.join(oi, 'org_id', 'left') \
                                       .select(ed_nodup_nosb["*"],oi['corp_id'], oi['corp_pll_name_e_desc'], 
                                       oi['adm_region_id'], oi[ 'adm_region_pll_name_e_desc'], oi[ 'adm_prov_id'],\
                                       oi[ 'adm_prov_plldj_name_e_desc'],oi[ 'acute_peer_group_code']\
                                    ,oi[ 'acute_peer_group_e_desc'],oi['reporting_layer_code'],oi_alias['reporting_layer_e_desc'],oi['nat_id'])
     # if corp_id is blank than replace with org_id
    ed_nodup_nosb_aa = ed_nodup_nosb_aa.withColumn('corp_id', F.when(ed_nodup_nosb_aa.corp_id.isNull(), ed_nodup_nosb_aa.org_id).otherwise(ed_nodup_nosb_aa.corp_id))
        
        # if corp_name is blank than replace with org_name
    ed_nodup_nosb_aa = ed_nodup_nosb_aa.withColumn('corp_pll_name_e_desc', F.when(ed_nodup_nosb_aa.corp_id.isNull(), ed_nodup_nosb_aa.corp_pll_name_e_desc) \
                                           .otherwise(ed_nodup_nosb_aa.corp_pll_name_e_desc))
    #remove UCC for corp level
    ed_nodup_nosb_noucc = ed_nodup_nosb_aa.filter(~col('FACILITY_AM_CARE_NUM').isin([row['FACILITY_AM_CARE_NUM'] for row in ucc.select('FACILITY_AM_CARE_NUM').collect()]))
    
    ed_record_admit_with_ucc = ed_nodup_nosb_aa.filter(col('VISIT_DISPOSITION').isin(['61', '63']))
    ed_record_admit_noucc = ed_nodup_nosb_noucc.filter(col('VISIT_DISPOSITION').isin(['61', '63']))
    #For corp level remove DQ,PS and standalone-standalone is not ready, I created list manually, DQ is dropped from 2024
    for_corp= ed_record_admit_noucc.filter(~col('FACILITY_AM_CARE_NUM').isin([row['facility_am_care_num'] for row in ps.select('facility_am_care_num').collect()]))\
    .filter(~col('FACILITY_AM_CARE_NUM').isin([row['FACILITY_AM_CARE_NUM'] for row in alone.select('FACILITY_AM_CARE_NUM').collect()]))
    # For peer group remove UCC and standalones only
    for_peer= ed_record_admit_noucc.filter(~col('FACILITY_AM_CARE_NUM').isin([row['FACILITY_AM_CARE_NUM'] for row in alone_a.select('FACILITY_AM_CARE_NUM').collect()]))
    # for NAT, PROV, REG level-include UCC and remove DQ-dropped in 2024
    #same dataframe will be used --ed_record_admit_with_ucc
    for_NATPROVREG_a = ed_record_admit_with_ucc.select("*")
    # find out suppressions
    
    TPIA_org_cnt = for_corp.groupby('SUBMISSION_FISCAL_YEAR', 'CORP_ID') \
        .agg(sum(when(col('WAIT_TIME_TO_INPATIENT_HOURS').isNotNull(), 1).otherwise(0)).alias('tpia_rec'))
    # Splitting tpia_org_cnt into tpia_rpt_org and tpia_supp_org
    tpia_supp_org = tpia_org_rec.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
    tpia_rpt_org = tpia_org_rec.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))

    # LOS_rpt_org = TPIA_org_cnt.filter(LOS_org_cnt['los_avl_cnt'] >= 50)
    # LOS_supp_org = TPIA_org_cnt.filter(LOS_org_cnt['los_avl_cnt'] < 50)
    
    # tpia_org_cnt_UCC
    tpia_org_cnt_UCC = ed_record_admit_with_ucc.groupby('SUBMISSION_FISCAL_YEAR', 'CORP_ID') \
        .agg(sum(when(col('WAIT_TIME_TO_INPATIENT_HOURS').isNotNull(), 1)).alias('tpia_calc_cnt')) \
       
    # Splitting LOS_org_cnt_UCC into LOS_rpt_org_ucc and LOS_supp_org_ucc
    tpia_supp_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
    tpia_rpt_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))
    
    
    # Append the ucc df to make a list
    tpia_supp_org.append(tpia_supp_org_ucc_22)
    tpia_supp_corp = reduce(lambda x, y: x.unionAll(y), tpia_supp_orgs)


print("done")

error:
---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_6269/1745477641.py in <cell line: 94>()
    240 
    241     # Append the ucc df to make a list
--> 242     tpia_supp_org.append(tpia_supp_org_ucc_22)
    243     tpia_supp_corp = reduce(lambda x, y: x.unionAll(y), tpia_supp_orgs)
    244 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in __getattr__(self, name)
   3121         """
   3122         if name not in self.columns:
-> 3123             raise AttributeError(
   3124                 "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
   3125             )

AttributeError: 'DataFrame' object has no attribute 'append'
