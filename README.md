# Parameters for the query read - optional parameters commented out

schema = "nacrs_enrich_wide"
table = "nacrs_encounter_wide"
columns = ["AM_CARE_KEY", "SUBMISSION_FISCAL_YEAR", "FACILITY_PROVINCE", "AMCARE_GROUP_CODE",
    "FACILITY_AM_CARE_NUM", "TRIAGE_DATE", "TRIAGE_TIME", "DATE_OF_REGISTRATION", "REGISTRATION_TIME",
    "DISPOSITION_DATE", "DISPOSITION_TIME", "VISIT_DISPOSITION", "WAIT_TIME_TO_PIA_HOURS",
    "LOS_HOURS", "WAIT_TIME_TO_INPATIENT_HOURS", "TIME_PHYSICAN_INIT_ASSESSMENT",
    "ED_VISIT_IND_CODE", "GENDER", "AGE_NUM"]
where = ED_VISIT_IND_CODE = "1" AND AMCARE_GROUP_CODE = "ED" AND WHERE fiscal_year = "2022"
# env = "uat"
# key_col = "table_primary_key"
# trim = True
# limit = 5

solve error below 

File "/tmp/ipykernel_1473/3514292656.py", line 10
    where = ED_VISIT_IND_CODE = "1" AND AMCARE_GROUP_CODE = "ED" AND WHERE fiscal_year = "2022"
                                    ^
SyntaxError: invalid syntax
