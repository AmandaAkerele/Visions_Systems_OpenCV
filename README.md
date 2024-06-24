sort out the error message 

# from typing import List, Optional
# from pyspark.sql import functions as F
# from rs_helper import read_redshift_into_df
# import numpy as np
# import pandas as pd
# import os
# import psycopg2
# import getpass
# import sys
# import pyspark
# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.utils import AnalysisException
# from pyspark.sql import functions as F
# from pyspark.sql.functions import *
# from pyspark import SparkContext, SparkConf
from datetime import datetime
# from functools import reduce
# from pyspark.sql.types import FloatType, ArrayType
# from pyspark.sql.window import Window
# from scipy.stats import linregress



# Main function for executing your automated logic - add other transformaiton functions above as needed
# This function is the entrypoint used by the common ETL framework
def execute_pipeline(environment: str,
             redshift_username: str,
             redshift_password: str,
             spark: Optional[SparkSession] = None,
             years: Optional[List[int]] = None,
             year_type: str = None,
             reporting_type: str = None):

# Create empty dictionary for intermediate dataframes (will be written as intermediate job output)
    intermediate_data_dict = {}


# # Date processing to determine the fiscal years range
# current_date = datetime.datetime.now()
# comparison_date = datetime.datetime(current_date.year, 3, 31)

# if current_date > comparison_date:
#     closed_year = current_date.year - 1
# else:
#     closed_year = current_date.year - 2

# Get current date
current_date = datetime.now()

# Determine the comparison date as March 31 of the current year
comparison_date = datetime(datetime.now().year, 3, 31)

# Date processing to determine the fiscal years range
if current_date > comparison_date:
    closed_year = current_date.year - 1
else:
    closed_year = current_date.year - 2


yr_list = [str(closed_year-4), str(closed_year-3), str(closed_year-2), str(closed_year-1), str(closed_year)]
df_nacrs = [None] * 5  # Initialize list with None values

# List of fiscal years to process
# years = [str(closed_year - i) for i in range(5)]

# List to collect DataFrames from each year's processing
stacked_all = []
tpia_org_all = []
tpia_reg_all = []
tpia_supp_reg_all = []
tpia_supp_org_all = []
tpia_supp_peer_all = []

# Data fetching for organizational dimensions (example given that it does not vary by year)
df_org_dim = read_redshift_into_df(
        spark=spark,
        rs_username=redshift_username,
        rs_password=redshift_password,
        schema="org_info",
        table="org_dim",
        env="prod"
    )

# Loop through the last five years
for index in range(0, 5):
    df_nacrs[index] = read_redshift_into_df(
        spark=spark,
        rs_username=redshift_username,
        rs_password=redshift_password,
        schema="nacrs_enrich_wide",
        table="nacrs_encounter_wide",
        columns=[ 'org_id','AM_CARE_KEY', 'SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'facility_site_name',
                 'AMCARE_GROUP_CODE','FACILITY_AM_CARE_NUM', 'TRIAGE_DATE', 'TRIAGE_TIME', 'DATE_OF_REGISTRATION',
                 'REGISTRATION_TIME', 'amcare_type_code',
                 'DISPOSITION_DATE', 'DISPOSITION_TIME', 'VISIT_DISPOSITION', 'WAIT_TIME_TO_PIA_HOURS',
                 'LOS_HOURS', 'WAIT_TIME_TO_INPATIENT_HOURS', 'TIME_PHYSICAN_INIT_ASSESSMENT', 'acute_care_number',
                 'ED_VISIT_IND_CODE', 'GENDER', 'AGE_NUM', 'facility_name', 'facility_postal_code',
               'submitting_facility_province','PATIENT_CIHI_REGION_CODE','PATIENT_CIHI_REGION_E_DESC','problem_01',	
                 'problem_02',	 'problem_03',	 'problem_04','problem_05',	 'problem_06',	 'problem_07',	 
                 'problem_08',	 'problem_09',	 'problem_10', 'submission_period'],
        where="WHERE fiscal_year = " + yr_list[index],
        env="prod"
    )
   
    
    # Step 1: filters the DataFrame `df_nacrs` to include only records with `amcare_type_code` equal to '10'
    amcare_filtered = df_nacrs[index].filter(col('amcare_type_code') == '10') \
        .groupBy('FACILITY_PROVINCE', 'facility_am_care_num', 'facility_name', 'org_id', 'submission_period') \
        .agg(count("*").alias("total_count"))
    
    # Step 2: Generates period ranges for QC/BC (1 to 13) and other provinces (1 to 12)
    qc_bc_periods = spark.createDataFrame([(i,) for i in range(1, 14)], ["submission_period"])
    other_periods = spark.createDataFrame([(i,) for i in range(1, 13)], ["submission_period"])
    
    # Step 3: Create full period ranges by cross joining with distinct facilities
    full_qc_bc = amcare_filtered.filter(col('FACILITY_PROVINCE').isin("QC", "BC")) \
        .select("facility_am_care_num", "facility_name", "org_id").distinct().crossJoin(qc_bc_periods)
    
    full_other = amcare_filtered.filter(~col('FACILITY_PROVINCE').isin("QC", "BC")) \
        .select("facility_am_care_num", "facility_name", "org_id").distinct().crossJoin(other_periods)
    
    # Step 4: Left join to include all periods
    joined_qc_bc = full_qc_bc.join(
        amcare_filtered.withColumnRenamed("facility_name", "facility_name_org").withColumnRenamed("org_id","org_id_org"), 
        ["facility_am_care_num", "submission_period"], "left_outer"
    )
    
    joined_other = full_other.join(
        amcare_filtered.withColumnRenamed("facility_name", "facility_name_org").withColumnRenamed("org_id",                                                                                 "org_id_org").filter(col('submission_period') < 13), 
        ["facility_am_care_num", "submission_period"], "left_outer"
    )
    
    # Step 5: Count zero periods for each facility. Find periods with `total_count` as `Null` 
    zero_submission_counts_qc_bc = joined_qc_bc.filter(col("total_count").isNull()) \
        .groupBy("facility_am_care_num", "facility_name", "org_id") \
        .agg(count("*").alias("Periods_with_zero_counts"))
    
    zero_submission_counts_other = joined_other.filter(col("total_count").isNull()) \
        .groupBy("facility_am_care_num", "facility_name", "org_id") \
        .agg(count("*").alias("Periods_with_zero_counts"))
    
    # Step 6: Combine the zero period counts for QC/BC and other provinces, and filters the facilities with two or more 
    partial_submission = zero_submission_counts_qc_bc.union(zero_submission_counts_other) \
        .filter(col("Periods_with_zero_counts") >= 1)
    
    partial_submission.select("facility_am_care_num", "facility_name", "org_id", "Periods_with_zero_counts")
    
    # Filter for specific facility_am_care_num
    ps = partial_submission.filter(partial_submission['facility_am_care_num'].isin(['88030', '55538','54267','54250']))
    # ps.show()
    
    nacrs_ed = df_nacrs[index].filter(
        (col("ED_VISIT_IND_CODE") == "1") & (col("AMCARE_GROUP_CODE") == "ED")
    )
    
    
    # Define filter for still birth
    stillbirth_code = 'P95'
    problem_col = [col for col in df_nacrs[index].columns if col.startswith('problem_')]
    
    def find_flag(codes):
        if not problem_col:
            return F.lit(False)  # No problem columns, return a false literal
        still_birth_flag = functools.reduce(lambda a, b: a | b, [F.rtrim(F.col(column_name)) == codes for column_name in 
                                                                 problem_col])
        return still_birth_flag
    
    stillbirth_flag = find_flag(stillbirth_code)
    nacrs_ed_1 = nacrs_ed.withColumn('stillbirth_flags', F.when(stillbirth_flag, 1).otherwise(0))
    
    stillbirth = nacrs_ed_1.filter(col('stillbirth_flags') == 1).select("AM_CARE_KEY")
    
    # Show the result
    # stillbirth.show()
    
    
    # Removing Stillbirths
    ed_nodup_nosb = df_nacrs[index].filter(
        (col('ED_VISIT_IND_CODE') == '1') &
        (col('AMCARE_GROUP_CODE') == 'ED') &
        (~col('AM_CARE_KEY').isin([row['AM_CARE_KEY'] for row in stillbirth.select('AM_CARE_KEY').collect()]))
    )
    
    ed_nodup_nosb_22 = ed_nodup_nosb.join(df_org_dim, 'org_id', 'left') \
        .select(
            ed_nodup_nosb["*"],
            df_org_dim['corp_id'],
            df_org_dim['corp_pll_name_e_desc'],
            df_org_dim['adm_region_id'],
            df_org_dim['adm_region_pll_name_e_desc'],
            df_org_dim['adm_prov_id'],
            df_org_dim['adm_prov_plldj_name_e_desc'],
            df_org_dim['acute_peer_group_code'],
            df_org_dim['acute_peer_group_e_desc'],
            df_org_dim['reporting_layer_code'],
            df_org_dim['reporting_layer_e_desc'],
            df_org_dim['nat_id']
        )
     # when corp_id is blank replace with org_id
    ed_nodup_nosb_22 = ed_nodup_nosb_22.withColumn('corp_id', F.when(ed_nodup_nosb_22.corp_id.isNull(),                                                               ed_nodup_nosb_22.org_id).otherwise(ed_nodup_nosb_22.corp_id))
        
        # when corp_name is blank replace with org_name
    ed_nodup_nosb_22 = ed_nodup_nosb_22.withColumn('corp_pll_name_e_desc', F.when(ed_nodup_nosb_22.corp_id.isNull(),
                                                                                  ed_nodup_nosb_22.corp_pll_name_e_desc) \
                                           .otherwise(ed_nodup_nosb_22.corp_pll_name_e_desc))
    
    # List of facility with UCC records
    
    ucc_records = ed_nodup_nosb_22.filter(ed_nodup_nosb_22.amcare_type_code == '11')
    
    ucc_fac = ucc_records.select('org_id', 'FACILITY_AM_CARE_NUM').dropDuplicates()
    
    numerator = ucc_records.groupby("org_id", "FACILITY_AM_CARE_NUM").count().withColumnRenamed('count', 'numerator')
    denominator = ed_nodup_nosb_22.groupby("ORG_ID", "FACILITY_AM_CARE_NUM").count().withColumnRenamed('count',                                                                                             'denominator').select('ORG_ID',
                                                                                                                                                                                                                                  'denominator')
    
    ucc_fac_count_1 = numerator.join(denominator, ['org_id'], "left")
    ucc_fac_count = ucc_fac_count_1.withColumn('ucc_pct', (ucc_fac_count_1.numerator/ucc_fac_count_1.denominator)*100)
    
    
    # Remove UCCs and Stillbirths for corp level~ ed_nodup_nosb_noucc
    ed_nodup_noucc_nosb_22 = ed_nodup_nosb_22.join(
        ucc_fac_count.select('FACILITY_AM_CARE_NUM'),
        on=['FACILITY_AM_CARE_NUM'],
        how='left_anti'
    )
    ed_nodup_noucc_nosb_22.count()
    
    
    # Group by 'CORP_ID' and count 'AM_CARE_KEY', then rename the column to 'ED_CNT'
    ED_CORP_22_df = ed_nodup_noucc_nosb_22.groupBy('CORP_ID').agg(F.count('AM_CARE_KEY').alias('ED_CNT'))
    
    # # Creating initial DataFrame from the data
    # SL = spark.createDataFrame(data, ['CORP_ID', 'FACILITY_AM_CARE_NUM', 'CNT_SITE'])
    
    # # Filtering data to exclude '54242' and creating SL_filtered DataFrame
    # SL_filtered = SL.filter(col('FACILITY_AM_CARE_NUM') != 54242)
    
    
    # TPIA with UCC 
    # Create ed_record_with_ucc_22 (TPIA with UCC for National, Provincial and Regional)
    ed_records_with_ucc_22_bb_df = ed_nodup_nosb_22.filter(~col('VISIT_DISPOSITION').isin(['61', '63']))
    # No DQ to exclude for 2024 (ed_record_with_ucc_22)
    ed_record_admit_with_ucc = ed_records_with_ucc_22_bb_df 
    ed_record_with_ucc_22 = ed_record_admit_with_ucc
    
    # TPIA without UCC
    ed_records_22_bb_df = ed_nodup_noucc_nosb_22.filter(~F.col('VISIT_DISPOSITION').isin(['61', '63']))
    ed_record_admit_noucc = ed_records_22_bb_df
    
    #for corp level
    #Exclusion criteria for corp level. Exclude PS & DQ. DQ is dropped from 2024
    ed_record = ed_record_admit_noucc.filter(~col('FACILITY_AM_CARE_NUM').isin([row['facility_am_care_num'] for row in 
                                                                            ps.select('facility_am_care_num').collect()]))
    
    # For the PEER group, remove UCC and standalone facilities
    # Create ed_record 22_Peer without filtering based on SL_filtered
    ed_record_22_Peer = ed_record_admit_noucc
    
    
    #Step 2_IND_TPIA_FY22
    # For corp level without UCC
    # Vectorized approach for 'tpia_rec' calculation
    
    
    # Apply conditions to create the 'tpia_rec' column
    ed_record = ed_record.withColumn(
        'tpia_rec',
        F.when(
            F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B'
        ).when(
            F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N'
        ).otherwise('Y')
    )
    
    # Filter out records where 'tpia_rec' is not 'B'
    tpia_org_cnt = ed_record.filter(F.col('tpia_rec') != 'B')
    
    # Group by and aggregate data
    tpia_org_rec = tpia_org_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
        F.count('AM_CARE_KEY').alias('Total_CASE'),
        F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
        F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
    ).withColumn(
        'tpia_rec_pct',
        F.col('tpia_calc_cnt') / F.col('Total_CASE')
    )
    
    # Filter DataFrame based on specific conditions for suppression and reporting
    tpia_supp_org = tpia_org_rec.filter(
        (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
    )
    
    tpia_rpt_org = tpia_org_rec.filter(
        (F.col('tpia_calc_cnt') >= 50) | ((F.col('tpia_calc_cnt') < 50) & (F.col('tpia_rec_pct') >= 0.75))
    )
    
    # Create 'tpia_rec' column
    ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn('tpia_rec', 
                                                            F.when(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT']
                                                                   == '9999', 'N')                                              .when(ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'].isNotNull() & 
    (ed_record_with_ucc_22['TIME_PHYSICAN_INIT_ASSESSMENT'] != '9999'), 'Y')
                                                              .otherwise('B'))
    
    # Filter out rows with 'tpia_rec' equal to "B"
    tpia_org_cnt_ucc_22 = ed_record_with_ucc_22.filter(ed_record_with_ucc_22['tpia_rec'] != 'B')
    
    # Aggregation
    tpia_org_rec_ucc_22 = tpia_org_cnt_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID').agg(
        F.count('AM_CARE_KEY').alias('Total_CASE'),
        F.sum(F.when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
        F.sum(F.when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
    ).withColumn('tpia_rec_pct', col('tpia_calc_cnt') / col('Total_CASE'))
    
    
    
    # Filter 'tpia_org_rec_ucc' based on conditions
    tpia_supp_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') < 50) | ((col('tpia_calc_cnt') > 50) & (col('tpia_rec_pct') < 0.75)))
    tpia_rpt_org_ucc_22 = tpia_org_rec_ucc_22.filter((col('tpia_calc_cnt') >= 50) | ((col('tpia_calc_cnt') < 50) & (col('tpia_rec_pct') >= 0.75)))
    
     # Append the ucc df to make a list
    tpia_supp_org_all.append(tpia_supp_org_ucc_22)
    
    # Stack all processed ucc using reduce and unionAll()
    tpia_supp_corp_combined = reduce(lambda x, y: x.unionAll(y), tpia_supp_org_all)
    
    ## For Peer level
    # Assuming ed_record_22_Peer is already defined and available DataFrame
    # Update tpia_rec based on conditions in TIME_PHYSICAN_INIT_ASSESSMENT column
    tpia_peer_cnt_a_df = ed_record_22_Peer.withColumn(
        'tpia_rec',
        when(
            (col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (col('TIME_PHYSICAN_INIT_ASSESSMENT') == '')), 'B'
        ).when(
            col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N'
        ).otherwise('Y')
    )
    
    # Filter out records where tpia_rec is not 'B'
    tpia_peer_cnt_df = tpia_peer_cnt_a_df.filter(col('tpia_rec') != 'B')
    
    # Group by and aggregate calculations
    tpia_peer_rec_df = tpia_peer_cnt_df.groupBy('SUBMISSION_FISCAL_YEAR', 'acute_peer_group_code').agg(
        _sum(when(col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
        _sum(when(col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
    ).withColumn(
        'tpia_rec_pct',
        col('tpia_calc_cnt') / col('tpia_elig_cnt')
    )
    
    # Filter for suppression check
    tpia_supp_peer_df = tpia_peer_rec_df.filter(col('tpia_calc_cnt') < 50)
     # Append the peer df to make a list
    tpia_supp_peer_all.append(tpia_supp_peer_df)
    
    # Stack all dataframes using reduce and unionAll()
    tpia_supp_peer_combined = reduce(lambda x, y: x.unionAll(y), tpia_supp_peer_all)
    
    # For Region
    # Apply conditions to create the 'tpia_rec' column
    ed_record_with_ucc_22 = ed_record_with_ucc_22.withColumn(
        'tpia_rec',
        F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 
               'B') \
        .when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N') \
        .otherwise('Y')
    )
    
    # Filter out records where 'tpia_rec' is not 'B'
    tpia_reg_cnt = ed_record_with_ucc_22.filter(F.col('tpia_rec') != 'B')
    
    # Group by and aggregate data
    tpia_reg_rec = tpia_reg_cnt.groupBy('SUBMISSION_FISCAL_YEAR', 'adm_region_id','adm_region_pll_name_e_desc').agg(
        F.count('AM_CARE_KEY').alias('Total_CASE'),
        F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
        F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
    ).withColumn(
        'tpia_rec_pct',
        F.col('tpia_calc_cnt') / F.col('Total_CASE')
    )
    
    # Remove rows where 'ads_region_id' is null
    tpia_reg_rec = tpia_reg_rec.filter(F.col('adm_region_id').isNotNull())
    
    # Filter DataFrame based on specific conditions for suppression and reporting
    tpia_supp_reg = tpia_reg_rec.filter(
        (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
    )
    
    tpia_rpt_reg = tpia_reg_rec.filter(
        (F.col('tpia_calc_cnt') >= 50) | ((F.col('tpia_calc_cnt') < 50) & (F.col('tpia_rec_pct') >= 0.75))
    )
    
     # Append the peer df to make a list
    tpia_supp_reg_all.append(tpia_supp_reg)
    
    # Stack all dataframes using reduce and unionAll()
    tpia_supp_reg_combined = reduce(lambda x, y: x.unionAll(y), tpia_supp_reg_all)
    
    
    # Exclusion Rule for Region, Province, and National
    tpia_org_supp_for_natprovreg_df = tpia_supp_org_ucc_22.filter(col('tpia_calc_cnt') < 5)
    
    # Collect the list of CORP_IDs to be excluded
    exclude_corp_ids = tpia_org_supp_for_natprovreg_df.select('CORP_ID').rdd.flatMap(lambda x: x).collect()
    
    # Filter out the rows from ed_record_with_ucc_22
    TPIA_nt_record_ucc_22 = ed_record_with_ucc_22.filter(~col('CORP_ID').isin(exclude_corp_ids))
    
    
    
    ## **STEP 3_IND_TPIA_FY22:**
    
    # National 90th percentile TPIA
    
    tpia_nt_22 = TPIA_nt_record_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR') \
                              .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
    # tpia_nt_22.show()
    
    # Provincial 90th percentile TPIA
    tpia_prov_22= TPIA_nt_record_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR','adm_prov_id', 'adm_prov_plldj_name_e_desc') \
                              .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
    
    # Filter out rows where 'adm_prov_id' is null after aggregation
    tpia_prov_22 = tpia_prov_22.filter(tpia_prov_22['adm_prov_id'].isNotNull())
    # tpia_prov_22.show()
    
    
    # Peer Group 90th  percentile TPIA (return back records for ed_record_22_Peer is higher above)
    tpia_peer_22= ed_record_22_Peer.groupBy('SUBMISSION_FISCAL_YEAR','acute_peer_group_code') \
                              .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
    # Filter out rows where 'adm_prov_id' is null after aggregation
    tpia_peer_22 = tpia_peer_22.filter(tpia_peer_22['acute_peer_group_code'].isNotNull())
    # tpia_peer_22.show()
    
    
    # Regional 90th percentile TPIA
    tpia_reg_22= ed_record_with_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR','adm_prov_plldj_name_e_desc', 'adm_region_id', 
                                               'adm_region_pll_name_e_desc') \
                              .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
    # Filter out rows where 'adm_prov_id' is null after aggregation
    tpia_reg_22 = tpia_reg_22.filter(tpia_reg_22['adm_region_id'].isNotNull())
    # tpia_reg_22.show()
    
    # Calculate for SITE_ID/NAME/PEER
    tpia_site_22= ed_record.groupBy('SUBMISSION_FISCAL_YEAR','FACILITY_AM_CARE_NUM', 'acute_peer_group_code') \
                              .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
    # Filter out rows where 'adm_prov_id' is null after aggregation
    tpia_site_22 = tpia_site_22.filter(tpia_site_22['FACILITY_AM_CARE_NUM'].isNotNull())
    # tpia_site_22.show()
    
    tpia_corp= ed_record.groupBy('SUBMISSION_FISCAL_YEAR', 'CORP_ID') \
                              .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
    
    # Alias the DataFrames directly in the join operation for clarity
    tpia_corp = tpia_corp.alias("tpia_corp").join(
        df_org_dim.alias("org_dim"),
        col("tpia_corp.CORP_ID") == col("org_dim.org_id"),  # Ensure the join condition is clear
        how='left'  # Specify the type of join
    ).select(
        col("tpia_corp.CORP_ID"),  # Selecting columns from the 'tpia_corp' DataFrame
        col("tpia_corp.SUBMISSION_FISCAL_YEAR"),
        col("tpia_corp.PERCENTILE_90"),
        col("org_dim.acute_peer_group_code")  # Selecting from the 'org_dim' DataFrame
    )
    
    # TPIA_site_Huron_Perth
    TPIA_site_Huron_Perth = tpia_site_22.filter(col('FACILITY_AM_CARE_NUM').isin([54074,54075,54102,54168]))
    
    
    # Rename columns
    TPIA_site_Huron_Perth = TPIA_site_Huron_Perth.selectExpr('SUBMISSION_FISCAL_YEAR', 'FACILITY_AM_CARE_NUM as CORP_ID', 
                                                             'PERCENTILE_90')
    
    # Append to tpia_corp
    tpia_org_22 = tpia_corp.unionByName(TPIA_site_Huron_Perth, allowMissingColumns=True)
    
    
    ## **STEP 4_IND_TPIA_FY22:**
    
    #Perform the join operation on tpia_org_22
    tpia_conditions = ~(col('CORP_ID').isin([row.CORP_ID for row in tpia_supp_org.collect()]))
    
    # Remove Huron Perth Healthcare Alliance, corps_id = 80228 from TPIA  
    tpia_org_22_ta = tpia_org_22.filter(tpia_conditions).filter((col('CORP_ID') != 80228))
    # tpia_org_ta.show() # recheck value over by 34values. need to recheck tpia_org_22
    tpia_org_all.append(tpia_org_22_ta)
    tpia_org_combined = reduce(lambda x, y: x.unionAll(y), tpia_org_all)
    
    #Perform Join operation 
    tpia_reg_22_ta = tpia_reg_22.join(
        tpia_supp_reg, 
        tpia_reg_22['adm_region_id'] == tpia_supp_reg['adm_region_id'],
        'left_anti'
    )
    
    tpia_reg_all.append(tpia_reg_22_ta)
    tpia_reg_combined = reduce(lambda x, y: x.unionAll(y), tpia_reg_all)
    
    tpia_peer_aa = tpia_peer_22.withColumn('ID', when(col('acute_peer_group_code') == 'T', 2)\
                                        .when(col('acute_peer_group_code') == 'H1', 3) .when(col('acute_peer_group_code') 
                                                                                             == 'H2', 4)\
                                        .when(col('acute_peer_group_code') == 'H3', 5) .otherwise(None) ) 
    
    
     # Rename ID columns to a common name,
    tpia_nt_22 = tpia_nt_22.withColumn("ORG_ID", lit(1))
    tpia_prov_22 = tpia_prov_22.withColumnRenamed("adm_prov_id", "ORG_ID")
    tpia_reg_22 = tpia_reg_22.withColumnRenamed("adm_region_id", "ORG_ID")
    tpia_org_22 = tpia_org_22.withColumnRenamed("CORP_ID", "ORG_ID").withColumnRenamed("acute_peer_group_code", 
                                                                                       "reporting_entity_code")
    tpia_peer_22= tpia_peer_aa.withColumnRenamed("ID", "ORG_ID").withColumnRenamed("acute_peer_group_code", 
                                                                                   "reporting_entity_code")
    
    # Define the common schema
    common_columns = ["ORG_ID", "SUBMISSION_FISCAL_YEAR","PERCENTILE_90","reporting_entity_code"]
    
    
     # Select the common columns or add missing columns with null values
    tpia_nt_22 = tpia_nt_22.select([col(c).alias(c) if c in tpia_nt_22.columns else lit(None).alias(c) for c in 
                                    common_columns])
    tpia_prov_22 = tpia_prov_22.select([col(c).alias(c) if c in tpia_prov_22.columns else lit(None).alias(c) for c in 
                                        common_columns])
    tpia_reg_22 = tpia_reg_22.select([col(c).alias(c) if c in tpia_reg_22.columns else lit(None).alias(c) for c in 
                                      common_columns])
    tpia_org_22 = tpia_org_22.select([col(c).alias(c) if c in tpia_org_22.columns else lit(None).alias(c) for c in 
                                      common_columns])
    tpia_peer_22 = tpia_peer_22.select([col(c).alias(c) if c in tpia_peer_22.columns else lit(None).alias(c) for c in 
                                        common_columns])
    
    # Union the DataFrames
    
    stacked_df = tpia_nt_22.unionByName(tpia_prov_22) \
                       .unionByName(tpia_reg_22) \
                       .unionByName(tpia_org_22) \
                       .unionByName(tpia_peer_22) \
                       .orderBy('ORG_ID')
        
    
    # Append the processed DataFrame to final_data list
    stacked_all.append(stacked_df)
    
    final_set = reduce(lambda x, y: x.unionByName(y), stacked_all)
    
    # Reformat and clean columns
    final_set = (final_set.withColumn("reporting_entity_code_text", col('ORG_ID').cast("string"))
                     .drop("reporting_entity_code", "ORG_ID")
                     .withColumnRenamed("reporting_entity_code_text", "reporting_entity_code"))

# Add new columns and ensure all transformations are done in a single chain to optimize
final_set_df = (final_set.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("submission_fiscal_year")))
                        .withColumn("reporting_entity_type_code", lit("ORG"))
                        .withColumn("indicator_code", lit("811"))
                        .withColumn("metric_code", lit("PCTL_90"))
                        .withColumn("breakdown_type_code_l1", lit("N/A"))
                        .withColumn("breakdown_value_code_l1", lit("N/A"))
                        .withColumn("breakdown_type_code_l2", lit("N/A"))
                        .withColumn("breakdown_value_code_l2", lit("N/A"))
                        .withColumn("breakdown_type_code_l3", lit("N/A"))
                        .withColumn("breakdown_value_code_l3", lit("N/A"))
                        .withColumn("metric_descriptor_group_code", lit(" "))
                        .withColumn("metric_descriptor_code", lit(" "))
                        .withColumn("missing_reason_code", lit(" "))
                        .withColumn("metric_result", col("PERCENTILE_90"))
                        .withColumn("public_metric_result", col("PERCENTILE_90"))
                        .withColumn("reporting_type_code", lit("Pub")))

# Update 'reporting_entity_code' based on specific values including the new mapping for ORG_ID 1 to 80235
final_set_df = final_set_df.withColumn("reporting_entity_code", 
    when(col("reporting_entity_code") == '1', "80235")
    .when(col("reporting_entity_code") == '2', "HPEER_T")
    .when(col("reporting_entity_code") == '3', "HPEER_H1")
    .when(col("reporting_entity_code") == '4', "HPEER_H2")
    .when(col("reporting_entity_code") == '5', "HPEER_H3")
    .otherwise(col("reporting_entity_code")))

# Re-arrange based on desired columns
desired_columns = [
    "reporting_period_code", "reporting_entity_code", "reporting_entity_type_code",
    "indicator_code", "metric_code", "breakdown_type_code_l1", "breakdown_value_code_l1",
    "breakdown_type_code_l2", "breakdown_value_code_l2", 
    "breakdown_type_code_l3","breakdown_value_code_l3","metric_result",
    "metric_descriptor_group_code", "metric_descriptor_code", "missing_reason_code",
    "public_metric_result", "reporting_type_code"
]
final_set_df = final_set_df.select(*desired_columns)

# Update 'reporting_entity_type_code' based on specific conditions
final_set_df = final_set_df.withColumn("reporting_entity_type_code",
    when(col("reporting_entity_code").isin(["HPEER_T", "HPEER_H1", "HPEER_H2", "HPEER_H3"]), "ORGCHAR")
    .otherwise(col("reporting_entity_type_code")))


def setup_dataframe(df, id_col, percentile_col):
    # Create a column to flag rows where the percentile is not null and count occurrences over years
    flagged_df = df.withColumn("Flag", F.when(F.col(percentile_col).isNotNull(), 1).otherwise(0))
    count_df = flagged_df.groupBy(id_col).agg(F.sum("Flag").alias("years_in_percentile"))

    # Filter IDs with at least 3 years in the percentile
    filtered_df = count_df.filter(F.col("years_in_percentile") >= 3)

    return flagged_df, filtered_df

def join_and_filter(df, filtered_df, id_col, closed_year):
    # Join back with the original DataFrame to get the full records for these IDs
    recent_year_df = df.join(filtered_df, on=id_col).filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    trend_years_df = df.join(filtered_df, on=id_col) \
                       .filter(F.col("SUBMISSION_FISCAL_YEAR").isin(
                           str(closed_year-1), str(closed_year-2), str(closed_year-3)))

    return recent_year_df, trend_years_df

#Comaprison Code

# Assuming tpia_org_combined and tpia_reg_combined are defined DataFrames
# Configure for corporate data
corp_all, corp_filtered = setup_dataframe(tpia_org_combined, "CORP_ID", "PERCENTILE_90")
corp_comp, corp_trend = join_and_filter(tpia_org_combined, corp_filtered, "CORP_ID", closed_year)

# Configure for regional data
reg_all, reg_filtered = setup_dataframe(tpia_reg_combined, "adm_region_id", "PERCENTILE_90")
reg_comp, reg_trend = join_and_filter(tpia_reg_combined, reg_filtered, "adm_region_id", closed_year)

# show results
# corp_comp.show()
# corp_trend.show()
# reg_comp.show()
# reg_trend.show()

# Filter the datasets for the specific fiscal year
tpia_reg_ta_final = tpia_reg_combined.filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year - 1))
tpia_corp_ta_final = tpia_org_combined.filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year - 1))

# Calculate both the 20th and 80th percentiles in a single groupBy operation for corporate data
tpia_peer_percentiles = tpia_corp_ta_final.groupBy("SUBMISSION_FISCAL_YEAR", "acute_peer_group_code").agg(
    F.percentile_approx("PERCENTILE_90", 0.2).alias("PERCENTILE_20"),
    F.percentile_approx("PERCENTILE_90", 0.8).alias("PERCENTILE_80")
)

# Join the 20th and 80th percentile data for easy access
tpia_peer_base = tpia_peer_percentiles

# Calculate both the 20th and 80th percentiles in a single groupBy operation for regional data
tpia_reg_percentiles = tpia_reg_ta_final.groupBy("SUBMISSION_FISCAL_YEAR").agg(
    F.percentile_approx("PERCENTILE_90", 0.2).alias("PERCENTILE_20"),
    F.percentile_approx("PERCENTILE_90", 0.8).alias("PERCENTILE_80")
)

# Join the 20th and 80th percentile data for easy access
tpia_reg_base = tpia_reg_percentiles

# Optional: Display the results for verification
# tpia_peer_base.show()
# tpia_reg_base.show()

# Create comparison datasets
# Alias the DataFrames for clarity
corp_comp_alias = corp_comp.alias("tpia_org_22")
tpia_peer_base_alias = tpia_peer_base.alias("tpia_peer_base")

# Perform the join and select the required columns
result_corp = corp_comp_alias.join(tpia_peer_base_alias, on="acute_peer_group_code", how="inner").select(
    col("tpia_org_22.CORP_ID"),
    col("tpia_org_22.SUBMISSION_FISCAL_YEAR"),
    col("tpia_org_22.PERCENTILE_90"),
    col("tpia_org_22.acute_peer_group_code"),
    col("tpia_peer_base.PERCENTILE_20"),
    col("tpia_peer_base.PERCENTILE_80")
)

# Add columns with conditions
comparison_columns = result_corp.withColumn("COMPARE_IND_CODE", 
    when(col("PERCENTILE_90") < col("PERCENTILE_20"), "001")
    .when((col("PERCENTILE_90") >= col("PERCENTILE_20")) & (col("PERCENTILE_90") <= col("PERCENTILE_80")), "002")
    .when(col("PERCENTILE_90") > col("PERCENTILE_80"), "003")
    .otherwise(" "))\
    .withColumn("metric_descriptor_code", 
    when(col("COMPARE_IND_CODE") == "001", "Above")
    .when(col("COMPARE_IND_CODE") == "002", "Same")
    .when(col("COMPARE_IND_CODE") == "003", "Below")
    .otherwise(" "))\
    .withColumn("metric_descriptor_group_code", 
    when(col("COMPARE_IND_CODE").isNotNull(), "PerformanceComparison")
    .otherwise(" "))\
    .withColumn("missing_reason_code", lit(" "))

# Final DataFrame for corp_comp
corp_comp_final = comparison_columns.withColumn("reporting_period_code", concat(lit("FY"),
                                                                                col("SUBMISSION_FISCAL_YEAR")))\
    .withColumnRenamed("CORP_ID", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG "))\
    .withColumn("indicator_code", lit("811"))\
    .withColumn("metric_code", lit("PCTL_90"))\
    .withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A"))\
    .withColumn("breakdown_type_code_l2", lit("N/A"))\
    .withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("breakdown_type_code_l3", lit("N/A"))\
    .withColumn("breakdown_value_code_l3", lit("N/A"))\
    .withColumn("metric_result", lit(0))\
    .withColumn("public_metric_result", lit(0))\
    .drop("PERCENTILE_20", "PERCENTILE_80", "acute_peer_group_code", "COMPARE_IND_CODE", "PERCENTILE_90", 
          "SUBMISSION_FISCAL_YEAR")\
    .withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))\
    .drop("reporting_entity_code")\
    .withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")\
    .withColumn("reporting_type_code", lit("Pub"))\
    .select(
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "breakdown_type_code_l3",
        "breakdown_value_code_l3",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result",
        "reporting_type_code"
    )

# Join and select columns for reg_comp
result_reg = reg_comp.join(tpia_reg_base, on="SUBMISSION_FISCAL_YEAR", how="inner").select("*")

# Add columns with conditions for reg_comp
comparison_columns_reg = result_reg.withColumn("COMPARE_IND_CODE", 
    when(col("PERCENTILE_90") < col("PERCENTILE_20"), "001")
    .when((col("PERCENTILE_90") >= col("PERCENTILE_20")) & (col("PERCENTILE_90") <= col("PERCENTILE_80")), "002")
    .when(col("PERCENTILE_90") > col("PERCENTILE_80"), "003")
    .otherwise(" "))\
    .withColumn("metric_descriptor_code", 
    when(col("COMPARE_IND_CODE") == "001", "Above")
    .when(col("COMPARE_IND_CODE") == "002", "Same")
    .when(col("COMPARE_IND_CODE") == "003", "Below")
    .otherwise(" "))\
    .withColumn("metric_descriptor_group_code", 
    when(col("COMPARE_IND_CODE").isNotNull(), "PerformanceComparison")
    .otherwise(" "))\
    .withColumn("missing_reason_code", lit(" "))

# Final DataFrame for reg_comp
reg_comp_final = comparison_columns_reg.withColumn("reporting_period_code", concat(lit("FY"), 
                                                                                   col("SUBMISSION_FISCAL_YEAR")))\
    .withColumnRenamed("adm_region_id", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG "))\
    .withColumn("indicator_code", lit("811"))\
    .withColumn("metric_code", lit("PCTL_90_90"))\
    .withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A"))\
    .withColumn("breakdown_type_code_l2", lit("N/A"))\
    .withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("breakdown_type_code_l3", lit("N/A"))\
    .withColumn("breakdown_value_code_l3", lit("N/A"))\
    .withColumn("metric_result", lit(0))\
    .withColumn("public_metric_result", lit(0))\
    .drop("PERCENTILE_20", "PERCENTILE_80", "acute_peer_group_code", "COMPARE_IND_CODE", "PERCENTILE_90", 
          "SUBMISSION_FISCAL_YEAR")\
    .withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))\
    .drop("reporting_entity_code")\
    .withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")\
    .withColumn("reporting_type_code", lit("Pub"))\
    .select(
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "breakdown_type_code_l3",
        "breakdown_value_code_l3",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result",
        "reporting_type_code"
    )

# # Validation steps
# corp_comp_final.show(5)
# reg_comp_final.show(5)

# Define a UDF for performing linear regression using scipy
def perform_regression(times, percentiles):
    times_float = np.array(times, dtype=np.float64)
    percentiles_float = np.array(percentiles, dtype=np.float64)
    if len(times_float) <= 1 or len(percentiles_float) <= 1:
        return [None, None, None, None]
    slope, intercept, r_value, p_value, std_err = linregress(times_float, percentiles_float)
    return [float(slope), float(intercept), float(p_value), float(std_err)]

schema = ArrayType(FloatType())
regression_udf = udf(perform_regression, schema)

# Function to process trend data
def process_trend_data(trend_df, id_col):
    grouped_data = trend_df.groupBy(id_col).agg(
        collect_list(col("SUBMISSION_FISCAL_YEAR")).alias("times"),
        collect_list(col("PERCENTILE_90")).alias("percentiles")
    )
    trend_with_regression = grouped_data.withColumn("regression_results", regression_udf(col("times"), 
                                                                                         col("percentiles")))

    # Expand the results into separate columns
    trend_with_columns = trend_with_regression.select(
        id_col,
        col("regression_results").getItem(0).alias("slope"),
        col("regression_results").getItem(1).alias("intercept"),
        col("regression_results").getItem(2).alias("p_value"),
        col("regression_results").getItem(3).alias("std_err")
    )

    # Define improvement indicators
    trend_with_columns = trend_with_columns.withColumn("IMPROVEMENT_IND_CODE", lit("002"))\
                                           .withColumn("metric_descriptor_code", lit("NoChange"))\
                                           .withColumn("missing_reason_code", lit(" "))

    # Applying the logic from the provided method
    mask_positive_slope = (trend_with_columns['p_value'] < 0.05) & (trend_with_columns['slope'] > 0)
    mask_negative_slope = (trend_with_columns['p_value'] < 0.05) & (trend_with_columns['slope'] < 0)

    trend_with_columns = trend_with_columns.withColumn("IMPROVEMENT_IND_CODE", when(mask_positive_slope,                                                               lit("003")).otherwise(col("IMPROVEMENT_IND_CODE")))\
                                           .withColumn("metric_descriptor_code", when(mask_positive_slope,
                                             lit("Weaken")).otherwise(col("metric_descriptor_code")))\
                                           .withColumn("IMPROVEMENT_IND_CODE", when(mask_negative_slope,
                                            lit("001")).otherwise(col("IMPROVEMENT_IND_CODE")))\
                                           .withColumn("metric_descriptor_group_code",
                                                       when(col("IMPROVEMENT_IND_CODE").isNotNull(),
                                                            "PerformanceTrend").otherwise(" "))\
                                           .withColumn("metric_descriptor_code", when(mask_negative_slope,                                                             lit("Improving")).otherwise(col("metric_descriptor_code")))
    trend_final = trend_with_columns.select(id_col, "IMPROVEMENT_IND_CODE", "metric_descriptor_code", 
                                            "metric_descriptor_group_code", "missing_reason_code")\
                                    .withColumn("SUBMISSION_FISCAL_YEAR", lit(str(closed_year - 1)))\
                                    .withColumn("reporting_period_code", concat(lit("FY"),
                                                            col("SUBMISSION_FISCAL_YEAR")))\
                                    .withColumnRenamed(id_col, "reporting_entity_code")\
                                    .withColumn("reporting_entity_type_code", lit("ORG "))\
                                    .withColumn("indicator_code", lit("811"))\
                                    .withColumn("metric_code", lit("PCTL_90"))\
                                    .withColumn("breakdown_type_code_l1", lit("N/A"))\
                                    .withColumn("breakdown_value_code_l1", lit("N/A"))\
                                    .withColumn("breakdown_type_code_l2", lit("N/A"))\
                                    .withColumn("breakdown_value_code_l2", lit("N/A"))\
                                    .withColumn("breakdown_type_code_l3", lit("N/A"))\
                                    .withColumn("breakdown_value_code_l3", lit("N/A"))\
                                    .withColumn("metric_result", lit(0))\
                                    .withColumn("public_metric_result", lit(0))\
                                    .drop("SUBMISSION_FISCAL_YEAR")\
                                    .drop("IMPROVEMENT_IND_CODE")\
                                    .withColumn("reporting_entity_code_text",
                                                col('reporting_entity_code').cast("string"))\
                                    .drop("reporting_entity_code")\
                                    .withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")\
                                    .withColumn("reporting_type_code", lit("Pub"))  # Add the reporting_type_code   
    #column at the end
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "breakdown_type_code_l3",
        "breakdown_value_code_l3",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result",
        "reporting_type_code"
    ]

    return trend_final.select(*desired_columns)

# Process corp_trend and reg_trend DataFrames
corp_trend_final = process_trend_data(corp_trend, "CORP_ID")
reg_trend_final = process_trend_data(reg_trend, "adm_region_id")

# Uncomment the lines below to show the schema or to display the final DataFrames
# corp_trend_final.printSchema()
# corp_trend_final.show()
# reg_trend_final.printSchema()
# reg_trend_final.show()

# Ensure the schema of all DataFrames is the same
def unify_schema(df: DataFrame, schema: list) -> DataFrame:
    for col_name in schema:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("string"))
    return df.select(schema)

# Define the unified schema
unified_schema = [
    "reporting_period_code",
    "reporting_entity_code",
    "reporting_entity_type_code",
    "indicator_code",
    "metric_code",
    "breakdown_type_code_l1",
    "breakdown_value_code_l1",
    "breakdown_type_code_l2",
    "breakdown_value_code_l2",
    "breakdown_type_code_l3",
    "breakdown_value_code_l3",
    "metric_result",
    "metric_descriptor_group_code",
    "metric_descriptor_code",
    "missing_reason_code",
    "public_metric_result",
    "reporting_type_code"
]

# Unify the schema for each DataFrame
final_set_df_unified = unify_schema(final_set_df, unified_schema)
reg_trend_final_unified = unify_schema(reg_trend_final, unified_schema)
corp_trend_final_unified = unify_schema(corp_trend_final, unified_schema)
corp_comp_final_unified = unify_schema(corp_comp_final, unified_schema)
reg_comp_final_unified = unify_schema(reg_comp_final, unified_schema)

# Union the DataFrames and order by 'reporting_period_code' and 'reporting_entity_code'
df_811 = (final_set_df_unified
  .unionByName(reg_trend_final_unified)
  .unionByName(corp_trend_final_unified)
  .unionByName(corp_comp_final_unified)
  .unionByName(reg_comp_final_unified)
  .orderBy('reporting_period_code', 'reporting_entity_code'))

# Filter out rows where 'reporting_entity_code' is not null
agg_811 = df_811.filter(col("reporting_entity_code").isNotNull())



## Save intermediate or additional data for business area usage/reference (e.g.: for external data requests, DQ 
#verification)
intermediate_data_dict["agg_811" + str(closed_year) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = agg_811
intermediate_data_dict[
"tpia_org_combined" + str(closed_year) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
] = tpia_org_combined
intermediate_data_dict[
"tpia_reg_combined" + str(closed_year) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
] = tpia_reg_combined
intermediate_data_dict[
"tpia_supp_reg_combined" + str(closed_year) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
] = tpia_supp_reg_combined
intermediate_data_dict[
"tpia_supp_corp_combined" + str(closed_year) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
] = tpia_supp_corp_combined
intermediate_data_dict[
"tpia_supp_peer_combined" + str(closed_year) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
] = tpia_supp_peer_combined



## Generate an output_df in a common format for your project
# For indicator automation, output_df shares the same layout and requirements as shallow slice
output_df = agg_811.select('*')

# Return both output_df and dictionary of intermediate dataframes
return output_df, intermediate_data_dict



                                                                                
---------------------------------------------------------------------------
NameError                                 Traceback (most recent call last)
/tmp/ipykernel_1007/711004049.py in <cell line: 947>()
    945 ## Save intermediate or additional data for business area usage/reference (e.g.: for external data requests, DQ
    946 #verification)
--> 947 intermediate_data_dict["agg_811" + str(closed_year) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = agg_811
    948 intermediate_data_dict[
    949 "tpia_org_combined" + str(closed_year) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')

NameError: name 'intermediate_data_dict' is not defined
