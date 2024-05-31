convert to pyspark 

# LOS for admit with UCC 
ed_records_admit_with_ucc_22_bb_df = ed_record_with_ucc_22_aa[ed_record_with_ucc_22_aa['VISIT_DISPOSITION'].isin(['06', '07'])]
