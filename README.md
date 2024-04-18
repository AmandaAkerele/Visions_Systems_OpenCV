los_site= ed_record_admit.groupby(['SUBMISSION_FISCAL_YEAR', 'SITE_ID','SITE_NAME', 'SITE_PEER'])['LOS_HOURS'].quantile(0.9).reset_index()
display(los_site)
