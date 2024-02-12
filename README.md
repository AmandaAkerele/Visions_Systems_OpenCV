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
sas.saslib('fac', path=r"L:\Groups\CAD\YHS InDepth\YHS In-Depth 2023 Nov Release\FACILITY_FILES\ED_FACILITY_LIST")
df_fac_a = sas.sasdata(table='ed_facility_list_final', libref='fac') 
df_ucc_a = sas.sasdata(table='ucc_2022', libref='fac') 
df_dq_a = sas.sasdata(table='submitting_fac_dq', libref='fac')
df_ps_a = sas.sasdata(table='partial_submitting_fac', libref='fac')
df_lookup_a = sas.sasdata(table='fy22_look_up_table', libref='fac')

df_nacrs_yr = nacrs_yr.to_df()
df_nacrs_yr=df_nacrs_yr.rename(columns=lambda x: x.upper())
df_dups = df_dups_a.to_df()
df_dups=df_dups.rename(columns=lambda x: x.upper())
df_fac=df_fac_a.to_df()
df_fac=df_fac.rename(columns=lambda x: x.upper())
df_ucc=df_ucc_a.to_df()
df_ucc=df_ucc.rename(columns=lambda x: x.upper())
df_dq=df_dq_a.to_df()
df_dq=df_dq.rename(columns=lambda x: x.upper()).astype({'FACILITY_AM_CARE_NUM':object, 'FISCAL_YEAR':object})
df_ps_test=df_ps_a.to_df()
df_ps_test['fiscal_year']=df_ps_test['fiscal_year'].astype(int)
df_ps_test['FACILITY_AM_CARE_NUM']=df_ps_test['FACILITY_AM_CARE_NUM'].astype(int)
df_ps=df_ps_test.rename(columns=lambda x: x.upper()).astype({'FACILITY_AM_CARE_NUM':object,'FISCAL_YEAR':object})
df_lookup=df_lookup_a.to_df()
df_lookup=df_lookup.rename(columns=lambda x: x.upper()).astype({'INSTNUM':object})
