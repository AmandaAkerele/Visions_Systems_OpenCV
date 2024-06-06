in the code below 

where adm_prov_id is null remove the row 



# Provincial 90th percentile TPIA
tpia_prov_22= TPIA_nt_record_ucc_22.groupBy('SUBMISSION_FISCAL_YEAR','adm_prov_id', 'adm_prov_plldj_name_e_desc') \
                          .agg(F.percentile_approx('WAIT_TIME_TO_PIA_HOURS', 0.9).alias('PERCENTILE_90'))
tpia_prov_22.show()
