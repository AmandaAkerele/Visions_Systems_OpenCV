nacrs_options={'keep':'AM_CARE_KEY SUBMISSION_FISCAL_YEAR FACILITY_PROVINCE AMCARE_GROUP_CODE \
                     FACILITY_AM_CARE_NUM TRIAGE_DATE TRIAGE_TIME DATE_OF_REGISTRATION REGISTRATION_TIME\
                     DISPOSITION_DATE DISPOSITION_TIME VISIT_DISPOSITION WAIT_TIME_TO_PIA_HOURS\
                     LOS_HOURS WAIT_TIME_TO_INPATIENT_HOURS TIME_PHYSICAN_INIT_ASSESSMENT\
                     ED_VISIT_IND_CODE AMCARE_GROUP_CODE GENDER AGE_NUM',
              'where': 'ED_VISIT_IND_CODE in ("1") and AMCARE_GROUP_CODE in ("ED")'}                        
nacrs_yr = sas.sasdata(table='ambulatory_care',libref=f"NACRS{open_year-2000}", dsopts=nacrs_options) 
nacrs_options2={'keep':'AM_CARE_KEY'}                       
df_dups_a = sas.sasdata(table='nacrs_gud_dups_2022_2023', 
                        libref=f"NACRSGUD", dsopts=nacrs_options2)
df_nacrs_yr = nacrs_yr.to_df()
df_nacrs_yr=df_nacrs_yr.rename(columns=lambda x: x.upper())                      
