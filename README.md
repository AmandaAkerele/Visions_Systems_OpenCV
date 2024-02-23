# Exclusion Rule for Region, Province and National
tpia_org_supp_for_natprovreg_df = tpia_supp_org_ucc_22[tpia_supp_org_ucc_22['tpia_calc_cnt'] <5]
TPIA_nt_record_ucc_df = ed_record_with_ucc_22[~ed_record_with_ucc_22['CORP_ID'].isin(tpia_org_supp_for_natprovreg_df['CORP_ID'])]
