%%time
dataset = 'NACRS'

year = 2022

col_filters = [ "AM_CARE_KEY", "SUBMISSION_FISCAL_YEAR", "FACILITY_PROVINCE",
    "AMCARE_GROUP_CODE", "FACILITY_AM_CARE_NUM", "TRIAGE_DATE",
    "TRIAGE_TIME", "DATE_OF_REGISTRATION", "REGISTRATION_TIME",
    "DISPOSITION_DATE", "DISPOSITION_TIME", "VISIT_DISPOSITION",
    "WAIT_TIME_TO_PIA_HOURS", "LOS_HOURS", "WAIT_TIME_TO_INPATIENT_HOURS",
    "TIME_PHYSICAN_INIT_ASSESSMENT", "ED_VISIT_IND_CODE", "GENDER", "AGE_NUM"]


# Read the Parquet file into a PyArrow Table
yearly_pyarrow_table = pq.read_table('/home/cihiusr/Data/GUD/PROD/{}/Data/parquet/{}_gud_{}_{}.parquet'.format(dataset, dataset.lower(), year, year+1)
                                     , columns = col_filters)

# Convert the Table to a pandas DataFrame
yearly_df = yearly_pyarrow_table.to_pandas()
yearly_df 
