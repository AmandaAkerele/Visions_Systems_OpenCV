los_reg= ed_record_admit_with_ucc_22.groupby(['SUBMISSION_FISCAL_YEAR', 'NEW_REGION_ID', 'REGION_E_DESC'])['LOS_HOURS'].quantile(0.9).reset_index()
display(los_reg)
