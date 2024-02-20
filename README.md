The error `[AMBIGUOUS_REFERENCE] Reference 'SUBMISSION_FISCAL_YEAR' is ambiguous, could be: ['SUBMISSION_FISCAL_YEAR', 'SUBMISSION_FISCAL_YEAR']` occurs because there are two columns with the same name `SUBMISSION_FISCAL_YEAR` in the DataFrame `ed_records_22_aa_df`. This typically happens after a join operation where both DataFrames have a column with the same name, and both are included in the result.

To resolve this issue, you should:

1. Rename or drop one of the duplicate columns after the join operation, before the `select`.
2. Ensure that the column selection in the `select` statement references columns unambiguously.

Here's how you can modify your code:

### Step 1: Adjust Join and Rename Duplicate Column

After joining `ed_nodup_noucc_nosb_22` and `df_fac`, you should rename or drop the duplicate `SUBMISSION_FISCAL_YEAR` column. I'll demonstrate renaming one of them:


# Adjusting the join operation
ed_records_22_aa_df = ed_nodup_noucc_nosb_22.join(df_fac, 'FACILITY_AM_CARE_NUM', 'left')

# Renaming one of the duplicate columns
# This depends on which 'SUBMISSION_FISCAL_YEAR' you want to keep or rename
# Assuming you want to keep the one from ed_nodup_noucc_nosb_22 and rename the one from df_fac
ed_records_22_aa_df = ed_records_22_aa_df.withColumnRenamed('SUBMISSION_FISCAL_YEAR', 'SUBMISSION_FISCAL_YEAR_nodup')
```

### Step 2: Select Columns with Correct Names

When selecting columns, use the correct, unambiguous names:

```python
# Select specific columns
selected_columns = ['AM_CARE_KEY', 'SUBMISSION_FISCAL_YEAR_nodup', 'FACILITY_PROVINCE', 'FACILITY_AM_CARE_NUM', 
                    'TRIAGE_DATE', 'TRIAGE_TIME', 'DATE_OF_REGISTRATION', 'REGISTRATION_TIME', 'DISPOSITION_DATE', 
                    'DISPOSITION_TIME', 'VISIT_DISPOSITION', 'WAIT_TIME_TO_PIA_HOURS', 'LOS_HOURS', 
                    'WAIT_TIME_TO_INPATIENT_HOURS', 'TIME_PHYSICAN_INIT_ASSESSMENT', 'ED_VISIT_IND_CODE', 
                    'AMCARE_GROUP_CODE', 'GENDER', 'AGE_NUM', 'SITE_ID', 'SITE_NAME', 'SITE_PEER', 'CORP_ID', 
                    'CORP_NAME', 'CORP_PEER', 'NEW_REGION_ID', 'REGION_E_DESC', 'PROVINCE_ID', 'PROVINCE_NAME', 
                    'NACRS_ED_FLG']

ed_records_22_aa_df = ed_records_22_aa_df.select(*selected_columns)
